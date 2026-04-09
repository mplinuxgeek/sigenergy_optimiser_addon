# SigEnergy Solar Import & Export Control

Intelligent battery-preserving controller for SigEnergy EMS that prioritizes having enough battery to reach sunrise, avoids paid energy where possible, and earns from export/import opportunities without taking losses.

## Battery protection (Priority #1)
- Dynamic sunrise calculation: estimates SoC needed based on current load until sunrise + buffer
- Sunrise target equals calculated need with a reserve floor (can be relaxed for export)
- Day reserve: configurable minimum SoC floor during daytime
- Always preserve overnight capacity before exporting
- At night on battery-only (no import/export), stays in Maximum Self Consumption and caps PV max power to 0.1 kW to avoid unintended grid import

## Export profit maximization (Priority #2)
- Tiered export limits scaling with FIT between low/medium/high thresholds
- SoC-based scaling: gradual ramp-up as battery fills above required sunrise level
- EMS hysteresis: uses start/stop around the low FIT threshold to prevent flapping
- Price spike boost: optional price spike sensor drives full export when spike is on (demand window no longer blocks export)
- Mode: Command Discharging (PV First) - prioritizes solar over battery
- Morning Dump: optional pre-sunrise export window (configurable hours before sunrise) that pushes export to the highest safe rate while still honoring the minimum SoC floor

## Import optimization
- Negative-price import: charge at full inverter power for any negative price — maximise free energy while it lasts
- Smart top-up: gentle charge at very low prices, with forecast-aware daytime gating
- Mode: Command Charging (Grid First) for negative prices, (PV First) for cheap top-up

## Anti-flapping & stability
- Price hysteresis on mode switching
- SoC hysteresis buffer for mode decisions
- Minimum change thresholds to update limits
- Notification deduplication: prevents spam when at limits
- Session tracking: records kWh totals for export/import events

## Requirements
- Never import or export when the price results in a loss
- Avoid paid imports when possible; only import at very low prices or to protect the overnight reserve
- Prefer value-positive export/import opportunities to offset fixed costs over time
- Before late morning, hold charging by switching to Command Discharging (PV First) when PV forecast is high and negative prices are expected; keeps serving load from PV/battery without grid import unless prices are already sufficiently negative
- Defaults are prefilled for a standard SigEnergy + Amber/Solcast setup; override if your entity IDs differ
- Charge holdoff can be disabled via the “Enable Charge Holdoff” toggle if you want to bypass forecast-based morning holdoff
