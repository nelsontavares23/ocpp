"""Run the conformance suite against the simulated OCPP 1.6 charger of the tests.

The simulated charger does not implement every operation, so the verdict is
not asserted here; the test proves the runner executes end to end, publishes
its result (response, event, file) and restores the charge point afterwards.
"""

import asyncio
import contextlib
import os

import pytest
from pytest_homeassistant_custom_component.common import async_capture_events
import websockets

from custom_components.ocpp.const import DOMAIN as OCPP_DOMAIN, EVENT_CONFORMANCE
from homeassistant.exceptions import HomeAssistantError

from .charge_point_test import wait_ready
from .test_charge_point_v16 import ChargePoint


@pytest.mark.timeout(90)
@pytest.mark.parametrize(
    "setup_config_entry",
    [{"port": 9601, "cp_id": "CP_conf", "cms": "cms_conf"}],
    indirect=True,
)
@pytest.mark.parametrize("cp_id", ["CP_conf"])
@pytest.mark.parametrize("port", [9601])
async def test_conformance_runs_end_to_end(
    hass, socket_enabled, cp_id, port, setup_config_entry
):
    """The action runs every safe test, publishes the result and cleans up."""
    cs = setup_config_entry
    events = async_capture_events(hass, EVENT_CONFORMANCE)

    async with websockets.connect(
        f"ws://127.0.0.1:{port}/{cp_id}", subprotocols=["ocpp1.6"]
    ) as ws:
        client = ChargePoint(f"{cp_id}_client", ws)
        task = asyncio.create_task(client.start())
        try:
            await client.send_boot_notification()
            await wait_ready(cs.charge_points[cp_id])
            srv = cs.charge_points[cp_id]
            cpid = srv.settings.cpid

            response = await hass.services.async_call(
                OCPP_DOMAIN,
                "run_conformance",
                {"devid": cpid, "level": "safe", "wait": True, "message_timeout": 1},
                blocking=True,
                return_response=True,
            )

            names = {r["test"] for r in response["results"]}
            assert "WebSocket subprotocol" in names
            assert "GetConfiguration (all)" in names
            assert "Unknown action -> CallError" in names
            assert response["verdict"] in (
                "CONFORMANT",
                "CONFORMANT WITH RESERVATIONS",
                "NON-CONFORMANT",
            )
            assert response["charger"]["cpid"] == cpid
            # intrusive tests are listed, not run, at level safe
            assert any(
                r["status"] == "SKIP" and "requires level" in r["detail"]
                for r in response["results"]
            )
            assert os.path.isfile(response["report_file"])

            await hass.async_block_till_done()
            assert [e.data["cpid"] for e in events] == [cpid]

            # the observer removed its wrappers from the live charge point
            assert "_handle_call" not in srv.__dict__
            assert "route_message" not in srv.__dict__

            # a finished run frees the charger for the next one
            second = await hass.services.async_call(
                OCPP_DOMAIN,
                "run_conformance",
                {"devid": cpid, "wait": False},
                blocking=True,
                return_response=True,
            )
            assert second["started"] is True
            with pytest.raises(HomeAssistantError):
                await hass.services.async_call(
                    OCPP_DOMAIN,
                    "run_conformance",
                    {"devid": cpid},
                    blocking=True,
                    return_response=True,
                )
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await ws.close()
