# OCPP 1.6J conformance test

The integration can check a connected OCPP 1.6J charger against the standard
and give a verdict. It runs inside Home Assistant on the connection the
charger already has, so nothing else has to be set up.

This is a diagnostic aid. It does not replace the Open Charge Alliance's OCTT
certification.

## Running it

Open **Developer tools → Actions**, choose **Run OCPP 1.6 conformance test**
(`ocpp.run_conformance`) and run it, for example:

```yaml
action: ocpp.run_conformance
data:
  devid: <cpid>
  level: safe
  wait: true
```

| Field | Meaning |
| --- | --- |
| `level` | `safe` (default): read-only checks. `intrusive`: also changes state temporarily. `disruptive`: also Reset and UpdateFirmware. |
| `connector` | Connector for connector-scoped tests (default 1). |
| `diagnostics_url` | Needed to test `GetDiagnostics`; skipped without it. |
| `firmware_url` | Needed to test `UpdateFirmware` (level `disruptive`); skipped without it. |
| `message_timeout` | Seconds to wait for a message the charger should send after a request (default 15). |
| `wait` | `true` keeps the action open and returns the report. Otherwise the test runs in the background. |

Only OCPP 1.6 chargers are supported. One test can run per charger at a time.

## Where the result goes

* The action response (with `wait: true`).
* A JSON report in `<config>/ocpp_conformance/<cpid>_<timestamp>.json`.
* An `ocpp_conformance` event with `charge_point_id`, `cpid`, `verdict`,
  `summary` and `report_file`.
* A persistent notification, unless notifications are disabled for the charger.

The verdict is `NON-CONFORMANT` if any test failed, `CONFORMANT WITH
RESERVATIONS` if there are warnings or unsupported operations, and
`CONFORMANT` otherwise.

## What is checked

Every CSMS-initiated operation: ChangeAvailability, ChangeConfiguration,
ClearCache, DataTransfer, GetConfiguration, RemoteStartTransaction,
RemoteStopTransaction, Reset, UnlockConnector, GetDiagnostics, UpdateFirmware,
GetLocalListVersion, SendLocalList, ReserveNow, CancelReservation,
SetChargingProfile, GetCompositeSchedule, ClearChargingProfile and
TriggerMessage.

Beyond a valid reply, it checks that:

* the mandatory configuration keys of the Core profile, and of the Smart
  Charging and Local Auth List profiles when advertised, exist;
* unknown or read-only keys are refused, an unknown DataTransfer vendor
  answers `UnknownVendorId`, an unknown transaction cannot be remote-stopped,
  and an unknown action answers `NotImplemented`;
* a profile the charger does not advertise is refused, and one it advertises
  is not;
* each `TriggerMessage` is followed by the message it asked for;
* after a Reset the charger reboots and sends `BootNotification` again;
* every message the charger sends during the run passes schema validation.

## Things to know

* **Schema validation must be on.** With *Skip OCPP schema validation* enabled
  for the charger, malformed charger messages cannot be detected, and the
  report says so.
* **Intrusive tests touch the charger.** They toggle a connector's
  availability and restore it, reserve and cancel a reservation, set and clear
  a charging profile (id 9101, at the configured maximum current, so it does
  not limit charging), replace the local authorization list with one test tag,
  and try a remote start and stop. A vehicle must be plugged in for the remote
  start to be accepted.
* **Triggering a boot is not a reboot.** The run asks the charger to resend
  `BootNotification`, which the integration counts as a new charger
  generation, so a session limit shown by *Session Current Limit* is forgotten.
* **After a Reset** the integration may reload itself if the charger reports
  different measurands or connectors; the test then ends with a warning.
* Charger-initiated traffic that does not occur during the run (for example
  meter values with no transaction) cannot be judged.
