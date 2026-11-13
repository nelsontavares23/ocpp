"""OCPP 1.6J conformance runner.

Runs on top of the live ChargePoint objects of this integration, so the
charger needs no extra connection: the ``ocpp.run_conformance`` action sends
every CSMS-initiated OCPP 1.6 operation to a connected charger, observes and
schema-checks what the charger sends back on its own initiative, and produces
a verdict (event, notification, JSON file and, optionally, action response).

It is a diagnostic aid and does not replace OCA's OCTT certification.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import json
import logging
import os
from typing import Any
import uuid

from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
import ocpp.exceptions as oe
from ocpp.messages import MessageType, unpack
from ocpp.v16 import call
from websockets.exceptions import ConnectionClosed
from websockets.protocol import State

from .const import DOMAIN, EVENT_CONFORMANCE
from .enums import (
    ConfigurationKey as ckey,
    HAChargerDetails as cdet,
    HAChargerStatuses as cstat,
    OcppMisc as om,
)

_LOGGER: logging.Logger = logging.getLogger(__package__)

LEVELS = {"safe": 0, "intrusive": 1, "disruptive": 2}
PASS, WARN, FAIL, UNSUP, SKIP = "PASS", "WARN", "FAIL", "UNSUPPORTED", "SKIP"

CORE = om.feature_profile_core.value
FW = om.feature_profile_firmware.value
AUTH = om.feature_profile_auth.value
RES = om.feature_profile_reservation.value
SMART = om.feature_profile_smart.value
REM = om.feature_profile_remote.value

# Keys the OCPP 1.6 specification requires of every charger (Core profile).
_CORE_KEYS = (
    ckey.authorize_remote_tx_requests,
    ckey.clock_aligned_data_interval,
    ckey.connection_time_out,
    ckey.connector_phase_rotation,
    ckey.get_configuration_max_keys,
    ckey.heartbeat_interval,
    ckey.local_authorize_offline,
    ckey.local_pre_authorize,
    ckey.meter_values_aligned_data,
    ckey.meter_values_sampled_data,
    ckey.meter_value_sample_interval,
    ckey.number_of_connectors,
    ckey.reset_retries,
    ckey.stop_transaction_on_ev_side_disconnect,
    ckey.stop_transaction_on_invalid_id,
    ckey.stop_txn_aligned_data,
    ckey.stop_txn_sampled_data,
    ckey.supported_feature_profiles,
    ckey.transaction_message_attempts,
    ckey.transaction_message_retry_interval,
    ckey.unlock_connector_on_ev_side_disconnect,
)
# Keys required once the matching feature profile is advertised.
_PROFILE_KEYS = {
    SMART: (
        ckey.charge_profile_max_stack_level,
        ckey.charging_schedule_allowed_charging_rate_unit,
        ckey.charging_schedule_max_periods,
        ckey.max_charging_profiles_installed,
    ),
    AUTH: (
        ckey.local_auth_list_enabled,
        ckey.local_auth_list_max_length,
        ckey.send_local_list_max_length,
    ),
}
_UNSUPPORTED_EXC = tuple(
    e
    for e in (
        getattr(oe, "NotImplementedError", None),
        getattr(oe, "NotSupportedError", None),
    )
    if e
)
_UNSUPPORTED_STATUS = {"NotSupported", "NotImplemented"}

_RUNNING = "_conformance_running"
_LAST_REPORTS = "_conformance_reports"
_TEST_PROFILE_ID = 9101  # never one of the ids the integration itself uses
_TEST_RESERVATION_ID = 9001


def _val(x: Any) -> Any:
    """Return the string value of an enum, or the value itself."""
    return getattr(x, "value", x)


class MessageObserver:
    """Record what the charger sends, for the duration of one run.

    Wraps two methods on the live ChargePoint *instance* (no core change):
    ``_handle_call`` to count and schema-check charger-initiated messages, and
    ``route_message`` to catch the CallError answering our unknown-action probe.
    Both are removed again on exit, restoring the class methods.
    """

    def __init__(self, cp) -> None:
        """Initialize."""
        self.cp = cp
        self.counts: dict[str, int] = {}
        self.violations: list[str] = []
        self._raw: dict[str, asyncio.Future] = {}
        self._orig_handle = None
        self._orig_route = None

    def __enter__(self) -> MessageObserver:
        """Install the wrappers."""
        self._orig_handle = self.cp._handle_call
        self._orig_route = self.cp.route_message
        self.cp._handle_call = self._handle_call
        self.cp.route_message = self._route_message
        return self

    def __exit__(self, *exc) -> None:
        """Restore the original methods."""
        for name in ("_handle_call", "route_message"):
            self.cp.__dict__.pop(name, None)

    def expect_error(self, unique_id: str) -> asyncio.Future:
        """Return a future resolved by the CallError for this raw message id."""
        fut = asyncio.get_running_loop().create_future()
        self._raw[unique_id] = fut
        return fut

    async def _handle_call(self, msg) -> None:
        self.counts[msg.action] = self.counts.get(msg.action, 0) + 1
        try:
            await self._orig_handle(msg)
        except oe.OCPPError as ex:
            self.violations.append(f"{msg.action}: {type(ex).__name__}: {ex}")
            raise

    async def _route_message(self, raw) -> None:
        try:
            msg = unpack(raw)
        except Exception:  # noqa: BLE001 - malformed frames are the library's concern
            msg = None
        if (
            msg is not None
            and msg.message_type_id == MessageType.CallError
            and msg.unique_id in self._raw
        ):
            fut = self._raw.pop(msg.unique_id)
            if not fut.done():
                fut.set_result(msg)
            return
        await self._orig_route(raw)


class ConformanceRunner:
    """Execute the conformance suite against one connected 1.6 charge point."""

    def __init__(
        self,
        hass: HomeAssistant,
        cp,
        *,
        level: str = "safe",
        connector: int = 1,
        diagnostics_url: str | None = None,
        firmware_url: str | None = None,
        message_timeout: float = 15.0,
    ) -> None:
        """Initialize."""
        self.hass = hass
        self.cp = cp
        self.level = level
        self.level_n = LEVELS[level]
        self.connector = connector
        self.diagnostics_url = diagnostics_url
        self.firmware_url = firmware_url
        self.message_timeout = message_timeout
        self.results: list[dict] = []
        self.config: dict[str, dict] = {}
        self.profiles: set[str] | None = None
        self.obs: MessageObserver | None = None
        self.task: asyncio.Task | None = None
        self.started = datetime.now(tz=UTC)

    # ------------------------------------------------------------ plumbing
    def _add(self, profile: str, name: str, status: str, detail: str = "") -> None:
        self.results.append(
            {"profile": profile, "test": name, "status": status, "detail": detail}
        )
        _LOGGER.debug("conformance %s: [%s] %s %s", self.cp.id, status, name, detail)

    def _connected(self) -> bool:
        return self.cp._connection.state is State.OPEN

    def _advertised(self, profile: str) -> bool | None:
        if profile == CORE:
            return True
        return None if self.profiles is None else profile in self.profiles

    async def _wait(self, action: str, since: int, timeout: float | None = None):
        loop = asyncio.get_running_loop()
        end = loop.time() + (timeout or self.message_timeout)
        while loop.time() < end:
            if self.obs.counts.get(action, 0) > since:
                return True
            await asyncio.sleep(0.2)
        return False

    async def _send(self, payload):
        """Return (response, None) or (None, (kind, text))."""
        try:
            return await self.cp.call(payload, suppress=False), None
        except TimeoutError:
            wait = getattr(self.cp, "_response_timeout", "?")
            return None, ("timeout", f"no response within {wait}s")
        except _UNSUPPORTED_EXC as ex:
            return None, ("unsupported", type(ex).__name__)
        except ConnectionClosed:
            return None, ("closed", "connection closed")
        except oe.OCPPError as ex:
            return None, ("error", f"{type(ex).__name__}: {ex}")

    async def _exchange(self, profile, name, payload, ok=("Accepted",), warn=()):
        """Send, classify the answer, return the response (or None)."""
        resp, err = await self._send(payload)
        adv = self._advertised(profile)
        if err:
            kind, text = err
            if kind == "unsupported":
                if adv is False:
                    self._add(
                        profile,
                        name,
                        PASS,
                        "correctly refused (profile not advertised)",
                    )
                elif adv is True:
                    self._add(profile, name, FAIL, f"profile advertised but {text}")
                else:
                    self._add(profile, name, UNSUP, text)
            else:
                self._add(profile, name, FAIL, text)
            return None
        status = _val(getattr(resp, "status", None))
        if status in _UNSUPPORTED_STATUS:
            if adv is True:
                self._add(
                    profile, name, FAIL, f"profile advertised but status={status}"
                )
            else:
                self._add(
                    profile, name, UNSUP if adv is None else PASS, f"status={status}"
                )
        elif status is None or status in ok:
            self._add(
                profile, name, PASS, f"status={status}" if status else "valid response"
            )
        elif status in warn:
            self._add(profile, name, WARN, f"status={status}")
        else:
            self._add(profile, name, WARN, f"unexpected status={status}")
        return resp

    # --------------------------------------------------------------- tests
    async def _t_integration(self) -> None:
        if self.cp.settings.skip_schema_validation:
            self._add(
                CORE,
                "Schema validation of charger messages",
                WARN,
                "'Skip OCPP schema validation' is on for this charger: "
                "malformed charger messages cannot be detected",
            )
        else:
            self._add(CORE, "Schema validation of charger messages", PASS, "enabled")

    async def _t_protocol(self) -> None:
        sub = getattr(self.cp._connection, "subprotocol", None)
        self._add(
            CORE,
            "WebSocket subprotocol",
            PASS if sub == "ocpp1.6" else FAIL,
            sub if sub == "ocpp1.6" else f"negotiated={sub!r}, expected 'ocpp1.6'",
        )

    async def _t_boot(self) -> None:
        if not self.obs.counts.get("BootNotification"):
            prev = self.cp.triggered_boot_notification
            # As trigger_boot_notification does, so the integration does not
            # announce a reboot that did not happen.
            self.cp.triggered_boot_notification = True
            try:
                await self._send(
                    call.TriggerMessage(requested_message="BootNotification")
                )
                await self._wait("BootNotification", 0, 10)
            finally:
                self.cp.triggered_boot_notification = prev
        vendor = self.cp._metrics[(0, cdet.vendor)].value
        model = self.cp._metrics[(0, cdet.model)].value
        if vendor and model:
            self._add(CORE, "BootNotification data", PASS, f"{vendor} {model}")
        else:
            self._add(
                CORE, "BootNotification data", FAIL, "vendor/model never received"
            )

    async def _t_get_configuration(self) -> None:
        resp = await self._exchange(
            CORE, "GetConfiguration (all)", call.GetConfiguration()
        )
        if resp is None:
            return
        for item in resp.configuration_key or []:
            self.config[item["key"]] = item
        sfp = self.config.get(ckey.supported_feature_profiles.value, {}).get("value")
        if not sfp:
            r, _ = await self._send(
                call.GetConfiguration(key=[ckey.supported_feature_profiles.value])
            )
            if r is not None and r.configuration_key:
                sfp = r.configuration_key[0].get("value")
        if sfp:
            self.profiles = {p.strip() for p in sfp.split(",") if p.strip()}
        self._add(
            CORE,
            "SupportedFeatureProfiles",
            PASS if self.profiles else WARN,
            sfp or "not obtained; advertised-profile cross-checks disabled",
        )

        required = [k.value for k in _CORE_KEYS]
        for profile, keys in _PROFILE_KEYS.items():
            if self.profiles and profile in self.profiles:
                required += [k.value for k in keys]
        missing = []
        for key in required:
            if key in self.config:
                continue
            r, _ = await self._send(call.GetConfiguration(key=[key]))
            if r is not None and r.configuration_key:
                self.config[key] = r.configuration_key[0]
            else:
                missing.append(key)
        self._add(
            CORE,
            "Mandatory configuration keys",
            FAIL if missing else PASS,
            f"missing: {missing}" if missing else f"{len(required)} present",
        )

    async def _t_get_configuration_unknown(self) -> None:
        resp, err = await self._send(call.GetConfiguration(key=["XxUnknownKeyXx"]))
        if err:
            self._add(CORE, "GetConfiguration unknown key", FAIL, err[1])
        elif "XxUnknownKeyXx" in (resp.unknown_key or []):
            self._add(
                CORE, "GetConfiguration unknown key", PASS, "listed in unknownKey"
            )
        else:
            self._add(
                CORE, "GetConfiguration unknown key", FAIL, "not listed in unknownKey"
            )

    async def _t_change_configuration(self) -> None:
        key = ckey.meter_value_sample_interval.value
        cur = self.config.get(key)
        if cur and cur.get("value") is not None and not cur.get("readonly"):
            await self._exchange(
                CORE,
                "ChangeConfiguration (same value)",
                call.ChangeConfiguration(key=key, value=str(cur["value"])),
                ok=("Accepted", "RebootRequired"),
            )
        else:
            self._add(
                CORE,
                "ChangeConfiguration (same value)",
                SKIP,
                f"{key} unavailable or read-only",
            )
        resp, err = await self._send(
            call.ChangeConfiguration(key="XxUnknownKeyXx", value="1")
        )
        if err:
            self._add(CORE, "ChangeConfiguration unknown key", FAIL, err[1])
        else:
            s = _val(resp.status)
            self._add(
                CORE,
                "ChangeConfiguration unknown key",
                PASS if s == "NotSupported" else FAIL,
                f"status={s}, expected NotSupported",
            )
        ro = next((k for k, v in self.config.items() if v.get("readonly")), None)
        if ro:
            resp, err = await self._send(
                call.ChangeConfiguration(
                    key=ro, value=str(self.config[ro].get("value", ""))
                )
            )
            s = _val(resp.status) if resp else None
            self._add(
                CORE,
                "ChangeConfiguration read-only key",
                PASS if s in ("Rejected", "NotSupported") else FAIL,
                f"{ro}: status={s or err}, expected Rejected",
            )

    async def _t_clear_cache(self) -> None:
        await self._exchange(
            CORE, "ClearCache", call.ClearCache(), ok=("Accepted", "Rejected")
        )

    async def _t_data_transfer(self) -> None:
        resp, err = await self._send(
            call.DataTransfer(
                vendor_id="com.conformance.unknown", message_id="x", data="x"
            )
        )
        if err:
            self._add(CORE, "DataTransfer unknown vendor", FAIL, err[1])
            return
        s = _val(resp.status)
        self._add(
            CORE,
            "DataTransfer unknown vendor",
            PASS if s == "UnknownVendorId" else WARN,
            f"status={s}, expected UnknownVendorId",
        )

    async def _t_remote_stop_unknown(self) -> None:
        resp, err = await self._send(
            call.RemoteStopTransaction(transaction_id=2147480000)
        )
        if err:
            self._add(CORE, "RemoteStopTransaction unknown id", FAIL, err[1])
            return
        s = _val(resp.status)
        self._add(
            CORE,
            "RemoteStopTransaction unknown id",
            PASS if s == "Rejected" else FAIL,
            f"status={s}, expected Rejected",
        )

    async def _t_unknown_action(self) -> None:
        uid = uuid.uuid4().hex
        fut = self.obs.expect_error(uid)
        await self.cp._send(json.dumps([2, uid, "NotAnOcppAction", {}]))
        try:
            msg = await asyncio.wait_for(fut, 10)
        except TimeoutError:
            self._add(CORE, "Unknown action -> CallError", FAIL, "no response")
            return
        self._add(
            CORE,
            "Unknown action -> CallError",
            PASS if msg.error_code == "NotImplemented" else WARN,
            f"errorCode={msg.error_code}, expected NotImplemented",
        )

    async def _t_triggers(self) -> None:
        plan = [
            ("BootNotification", None),
            ("Heartbeat", None),
            ("MeterValues", self.connector),
            ("StatusNotification", self.connector),
            ("DiagnosticsStatusNotification", None),
            ("FirmwareStatusNotification", None),
        ]
        for msg, conn in plan:
            before = self.obs.counts.get(msg, 0)
            kw: dict[str, Any] = {"requested_message": msg}
            if conn:
                kw["connector_id"] = conn
            prev = self.cp.triggered_boot_notification
            if msg == "BootNotification":
                self.cp.triggered_boot_notification = True
            try:
                resp = await self._exchange(
                    REM, f"TriggerMessage {msg}", call.TriggerMessage(**kw)
                )
                if resp is not None and _val(resp.status) == "Accepted":
                    got = await self._wait(msg, before)
                    self._add(
                        REM,
                        f"  -> {msg} sent after trigger",
                        PASS if got else FAIL,
                        ""
                        if got
                        else f"accepted but not sent within {self.message_timeout}s",
                    )
            finally:
                self.cp.triggered_boot_notification = prev

    async def _t_unlock(self) -> None:
        await self._exchange(
            CORE,
            "UnlockConnector",
            call.UnlockConnector(connector_id=self.connector),
            ok=("Unlocked", "UnlockFailed"),
        )

    async def _t_availability(self) -> None:
        before = self.obs.counts.get("StatusNotification", 0)
        resp = await self._exchange(
            CORE,
            "ChangeAvailability Inoperative",
            call.ChangeAvailability(connector_id=self.connector, type="Inoperative"),
            ok=("Accepted", "Scheduled"),
            warn=("Rejected",),
        )
        try:
            if resp is not None and _val(resp.status) == "Accepted":
                await self._wait("StatusNotification", before, 10)
                s = self.cp._metrics[(self.connector, cstat.status_connector)].value
                self._add(
                    CORE,
                    "  -> StatusNotification Unavailable",
                    PASS if s == "Unavailable" else WARN,
                    f"connector status={s}",
                )
        finally:
            await self._exchange(
                CORE,
                "ChangeAvailability Operative",
                call.ChangeAvailability(connector_id=self.connector, type="Operative"),
                ok=("Accepted", "Scheduled"),
                warn=("Rejected",),
            )

    async def _t_remote_start_stop(self) -> None:
        before = self.obs.counts.get("StartTransaction", 0)
        # The integration authorizes its own remote tag, so the charger's
        # Authorize/StartTransaction are accepted by the normal handlers.
        resp = await self._exchange(
            CORE,
            "RemoteStartTransaction",
            call.RemoteStartTransaction(
                id_tag=self.cp._remote_id_tag, connector_id=self.connector
            ),
            warn=("Rejected",),
        )
        if resp is None or _val(resp.status) != "Accepted":
            self._add(
                CORE,
                "  -> StartTransaction after remote start",
                SKIP,
                "refused (vehicle plugged in? connector free?)",
            )
            return
        if not await self._wait(
            "StartTransaction", before, max(25, self.message_timeout)
        ):
            self._add(
                CORE, "  -> StartTransaction after remote start", FAIL, "not sent"
            )
            return
        tx = self.cp._active_tx.get(self.connector)
        self._add(
            CORE,
            "  -> StartTransaction after remote start",
            PASS,
            f"transactionId={tx}",
        )
        sb = self.obs.counts.get("StopTransaction", 0)
        await self._exchange(
            CORE,
            "RemoteStopTransaction",
            call.RemoteStopTransaction(transaction_id=int(tx or 0)),
        )
        got = await self._wait("StopTransaction", sb, max(25, self.message_timeout))
        self._add(
            CORE,
            "  -> StopTransaction after remote stop",
            PASS if got else FAIL,
            "" if got else "not sent",
        )

    async def _t_diagnostics(self) -> None:
        if not self.diagnostics_url:
            self._add(FW, "GetDiagnostics", SKIP, "pass diagnostics_url")
            return
        before = self.obs.counts.get("DiagnosticsStatusNotification", 0)
        resp = await self._exchange(
            FW, "GetDiagnostics", call.GetDiagnostics(location=self.diagnostics_url)
        )
        if resp is not None:
            got = await self._wait("DiagnosticsStatusNotification", before, 30)
            self._add(
                FW,
                "  -> DiagnosticsStatusNotification",
                PASS if got else WARN,
                "" if got else "none within 30s",
            )

    async def _t_update_firmware(self) -> None:
        if not self.firmware_url:
            self._add(FW, "UpdateFirmware", SKIP, "pass firmware_url")
            return
        before = self.obs.counts.get("FirmwareStatusNotification", 0)
        resp = await self._exchange(
            FW,
            "UpdateFirmware",
            call.UpdateFirmware(
                location=self.firmware_url,
                retrieve_date=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            ),
        )
        if resp is not None:
            got = await self._wait("FirmwareStatusNotification", before, 60)
            self._add(
                FW,
                "  -> FirmwareStatusNotification",
                PASS if got else WARN,
                "" if got else "none within 60s",
            )

    async def _t_local_list(self) -> None:
        resp = await self._exchange(
            AUTH, "GetLocalListVersion", call.GetLocalListVersion()
        )
        cur = getattr(resp, "list_version", -1) if resp else -1
        if resp is not None and not isinstance(cur, int):
            self._add(AUTH, "  listVersion is an integer", FAIL, repr(cur))
            cur = -1
        if self.level_n < 1:
            return
        newv = max(cur, 0) + 1
        r = await self._exchange(
            AUTH,
            "SendLocalList (Full, 1 tag)",
            call.SendLocalList(
                list_version=newv,
                update_type="Full",
                local_authorization_list=[
                    {"id_tag": "CONFTEST01", "id_tag_info": {"status": "Accepted"}}
                ],
            ),
            warn=("Failed", "VersionMismatch"),
        )
        if r is not None and _val(r.status) == "Accepted":
            r2, _ = await self._send(call.GetLocalListVersion())
            v = getattr(r2, "list_version", None)
            self._add(
                AUTH,
                "  version updated",
                PASS if v == newv else FAIL,
                f"got={v}, expected={newv}",
            )

    async def _t_reservation(self) -> None:
        exp = (datetime.now(UTC) + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
        resp = await self._exchange(
            RES,
            "ReserveNow",
            call.ReserveNow(
                connector_id=self.connector,
                expiry_date=exp,
                id_tag=self.cp._remote_id_tag,
                reservation_id=_TEST_RESERVATION_ID,
            ),
            warn=("Rejected", "Occupied", "Faulted", "Unavailable"),
        )
        if resp is not None and _val(resp.status) == "Accepted":
            await self._exchange(
                RES,
                "CancelReservation",
                call.CancelReservation(reservation_id=_TEST_RESERVATION_ID),
            )
        r, err = await self._send(call.CancelReservation(reservation_id=99999))
        if r is not None:
            self._add(
                RES,
                "CancelReservation unknown id",
                PASS if _val(r.status) != "Accepted" else FAIL,
                f"status={_val(r.status)}",
            )
        elif err and err[0] == "unsupported" and self._advertised(RES):
            self._add(RES, "CancelReservation unknown id", FAIL, err[1])

    async def _t_smart(self) -> None:
        if self.level_n >= 1:
            # A ceiling at the configured maximum current is a no-op for
            # charging, and id 9101 never collides with the integration's own.
            profile = {
                "chargingProfileId": _TEST_PROFILE_ID,
                "stackLevel": 0,
                "chargingProfilePurpose": "ChargePointMaxProfile",
                "chargingProfileKind": "Relative",
                "chargingSchedule": {
                    "chargingRateUnit": "A",
                    "chargingSchedulePeriod": [
                        {
                            "startPeriod": 0,
                            "limit": float(self.cp.settings.max_current),
                        }
                    ],
                },
            }
            await self._exchange(
                SMART,
                "SetChargingProfile",
                call.SetChargingProfile(connector_id=0, cs_charging_profiles=profile),
                warn=("Rejected",),
            )
        resp = await self._exchange(
            SMART,
            "GetCompositeSchedule",
            call.GetCompositeSchedule(
                connector_id=self.connector, duration=600, charging_rate_unit="A"
            ),
            warn=("Rejected",),
        )
        if (
            resp is not None
            and _val(resp.status) == "Accepted"
            and not getattr(resp, "charging_schedule", None)
        ):
            self._add(
                SMART, "  schedule present", WARN, "Accepted without chargingSchedule"
            )
        if self.level_n >= 1:
            await self._exchange(
                SMART,
                f"ClearChargingProfile (id {_TEST_PROFILE_ID})",
                call.ClearChargingProfile(id=_TEST_PROFILE_ID),
                ok=("Accepted", "Unknown"),
            )

    async def _t_reset(self) -> None:
        before = self.obs.counts.get("BootNotification", 0)
        resp = await self._exchange(CORE, "Reset (Soft)", call.Reset(type="Soft"))
        if resp is not None and _val(resp.status) == "Accepted":
            got = await self._wait("BootNotification", before, 180)
            self._add(
                CORE,
                "  -> charger rebooted and sent BootNotification",
                PASS if got else WARN,
                "" if got else "none within 180s",
            )

    # ------------------------------------------------------------- driver
    async def _suite(self) -> None:
        await asyncio.sleep(1)
        suite = [
            (0, self._t_integration),
            (0, self._t_protocol),
            (0, self._t_boot),
            (0, self._t_get_configuration),
            (0, self._t_get_configuration_unknown),
            (0, self._t_change_configuration),
            (0, self._t_clear_cache),
            (0, self._t_data_transfer),
            (0, self._t_remote_stop_unknown),
            (0, self._t_unknown_action),
            (0, self._t_triggers),
            (0, self._t_local_list),
            (0, self._t_smart),
            (0, self._t_diagnostics),
            (1, self._t_unlock),
            (1, self._t_availability),
            (1, self._t_remote_start_stop),
            (1, self._t_reservation),
            (2, self._t_update_firmware),
            (2, self._t_reset),
        ]
        for lvl, fn in suite:
            name = fn.__name__[3:]
            if not self._connected():
                self._add("-", name, SKIP, "connection closed")
            elif lvl > self.level_n:
                self._add("-", name, SKIP, f"requires level {list(LEVELS)[lvl]}")
            else:
                try:
                    await fn()
                except asyncio.CancelledError:
                    raise
                except Exception as ex:  # noqa: BLE001 - always produce a report
                    _LOGGER.exception("conformance test %s crashed", name)
                    self._add("-", name, FAIL, f"exception: {type(ex).__name__}: {ex}")
        for action, n in sorted(self.obs.counts.items()):
            self._add("Passive", f"Received {action}", PASS, f"{n}x")
        for violation in self.obs.violations:
            self._add("Passive", "Invalid message from charger", FAIL, violation)

    def _info(self) -> dict:
        feat = self.cp.supported_features
        return {
            "cp_id": self.cp.id,
            "cpid": self.cp.settings.cpid,
            "ocpp_version": self.cp._ocpp_version,
            "vendor": self.cp._metrics[(0, cdet.vendor)].value,
            "model": self.cp._metrics[(0, cdet.model)].value,
            "firmware": self.cp._metrics[(0, cdet.firmware_version)].value,
            "serial": self.cp._metrics[(0, cdet.serial)].value,
            "num_connectors": self.cp.num_connectors,
            "integration_features": getattr(feat, "labels", lambda: str(feat))(),
            "advertised_profiles": sorted(self.profiles) if self.profiles else None,
        }

    def _report(self) -> dict:
        summary: dict[str, int] = {}
        for r in self.results:
            summary[r["status"]] = summary.get(r["status"], 0) + 1
        if summary.get(FAIL):
            verdict = "NON-CONFORMANT"
        elif summary.get(WARN) or summary.get(UNSUP):
            verdict = "CONFORMANT WITH RESERVATIONS"
        else:
            verdict = "CONFORMANT"
        return {
            "verdict": verdict,
            "summary": summary,
            "level": self.level,
            "started": self.started.isoformat(),
            "finished": datetime.now(tz=UTC).isoformat(),
            "charger": self._info(),
            "results": self.results,
        }

    def _write(self, report: dict) -> str:
        directory = self.hass.config.path("ocpp_conformance")
        os.makedirs(directory, exist_ok=True)
        stamp = self.started.strftime("%Y%m%d_%H%M%S")
        path = os.path.join(directory, f"{self.cp.settings.cpid}_{stamp}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)
        return path

    async def run(self) -> dict:
        """Run the suite, then publish the result; always returns a report."""
        domain_data = self.hass.data.setdefault(DOMAIN, {})
        try:
            with MessageObserver(self.cp) as self.obs:
                await self._suite()
        finally:
            domain_data.get(_RUNNING, set()).discard(self.cp.id)
        report = self._report()
        try:
            report["report_file"] = await self.hass.async_add_executor_job(
                self._write, report
            )
        except OSError as ex:
            _LOGGER.warning("Could not write conformance report: %s", ex)
        domain_data.setdefault(_LAST_REPORTS, {})[self.cp.settings.cpid] = report
        self.hass.bus.async_fire(
            EVENT_CONFORMANCE,
            {
                "charge_point_id": self.cp.id,
                "cpid": self.cp.settings.cpid,
                "verdict": report["verdict"],
                "summary": report["summary"],
                "report_file": report.get("report_file"),
            },
        )
        await self.cp.notify_ha(
            f"Conformance test of {self.cp.settings.cpid}: {report['verdict']} "
            f"{report['summary']}. Report: {report.get('report_file', 'n/a')}",
            title="OCPP conformance",
        )
        return report


async def async_start_conformance(
    hass: HomeAssistant, cp, data: dict[str, Any]
) -> dict[str, Any]:
    """Start (or run to completion) a conformance test for a connected charger."""
    if cp._ocpp_version != "1.6":
        raise HomeAssistantError(
            translation_domain=DOMAIN,
            translation_key="conformance_wrong_version",
            translation_placeholders={"message": str(cp._ocpp_version)},
        )
    running: set = hass.data.setdefault(DOMAIN, {}).setdefault(_RUNNING, set())
    if cp.id in running:
        raise HomeAssistantError(
            translation_domain=DOMAIN,
            translation_key="conformance_running",
            translation_placeholders={"message": cp.id},
        )
    runner = ConformanceRunner(
        hass,
        cp,
        level=data.get("level", "safe"),
        connector=int(data.get("connector", 1)),
        diagnostics_url=data.get("diagnostics_url"),
        firmware_url=data.get("firmware_url"),
        message_timeout=float(data.get("message_timeout", 15)),
    )
    running.add(cp.id)
    if data.get("wait"):
        return await runner.run()
    runner.task = hass.async_create_background_task(
        runner.run(), f"ocpp_conformance_{cp.id}"
    )
    return {
        "started": True,
        "level": runner.level,
        "report_dir": hass.config.path("ocpp_conformance"),
    }
