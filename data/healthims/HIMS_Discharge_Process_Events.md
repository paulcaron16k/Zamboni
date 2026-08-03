# In-Patient Hospital Discharge — Process Events

> Converted from `HIMS_Discharge_Process_Events.docx`. Prose is reproduced as written;
> the `event_name` column in each table is the canonical identifier this demo uses and was
> derived from the headings, not present in the original.

The key events in a hospital in-patient discharge process are the discharge order,
medication reconciliation, and patient education. [1, 2, 3]

## Business context (from the source document)

### Discharge Planning and Orders

- **Doctor's order:** The main doctor writes the formal order to send the patient home.
- **Timing plan:** The team sets the expected date and time for the patient to leave.
- **Case management:** The social worker or nurse checks if the patient needs home care
  equipment or nursing help. [1, 2, 3]

### Medication and Instructions

- **Medication review:** Staff compare old and new medicines to make a safe final list.
- **Instruction review:** Nurses explain warning signs, diet rules, and activity limits to
  the patient.
- **Follow-up appointments:** Staff schedule visits with the main doctor or specialists.

### Final Checkout

- **Property return:** The patient gets back personal items kept by the hospital.
- **Paperwork:** Staff hand over the official discharge summary and prescription sheets.
- **Physical exit:** A staff member helps the patient leave the room and exit the building
  safely. [1]

---

## Internal HIMS-Tracked Events

Clinical and financial milestones.

| # | `event_name` | Description (source) | Actor role |
|---|---|---|---|
| 1 | `attending_doctor_initiated` | The primary physician reviews clinical metrics, determines the patient is medically stable, and submits the official electronic discharge order. | `doctor` |
| 2 | `floor_nurse_clearance` | The floor nurse completes a final physical assessment, ensures the patient understands their discharge instructions, and confirms necessary home-care medical equipment is ready. | `nurse-staff` |
| 3 | `pharma_return_started` | Nursing or unit staff pull unused, unopened patient-specific medications from the floor stock or automated dispensing cabinets and log them back into the tracking system. | `nurse-staff` |
| 4 | `pharma_return_received` | The hospital pharmacy receives, inspects, and logs the returned medications, officially adjusting the active inventory and updating the patient's record. [1] | `pharma-mgr` |
| 5 | `billing_initiated_insurance` | The billing department compiles the final itemized clinical codes, treatments, and medication logs to format and submit the official claim to the health insurance provider. [1, 2] | `billing-mgr` |
| 6 | `billing_insurance_received` | The insurance company receives the electronic claim, reviews the coverage limits, processes the pre-authorizations, and sends back an adjudication response or payment details. [1, 2] | `billing-mgr` |
| 7 | `billing_clearance` | The hospital finance office calculates the patient's final out-of-pocket responsibility, verifies the insurance payout, and applies the administrative clearance flag to the digital file. [1] | `billing-mgr` |

---

## Additional Events

> The previous list covers the clinical and financial milestones, but a hospital's
> throughput relies on the operational, logistics, and Environmental Services (EVS) events
> that happen in parallel. [1, 2]

### Logistics & Departure

| # | `event_name` | Description (source) | Actor role |
|---|---|---|---|
| 8 | `pending_discharge_logged` | The charge nurse updates the digital bed board to flag the room as a "predicted discharge," allowing the admissions and emergency departments to map incoming patients. [1, 2, 3] | `nurse-mgr` |
| 9 | `dietary_tray_canceled` | The unit clerk cancels the patient's meal plan in the hospitality system to prevent waste. [1] | `nurse-staff` |
| 10 | `patient_displaced_discharged` | The patient physically leaves the unit (often rolled out via a transport porter), and the nurse enters the formal departure time stamp into the Electronic Health Record (EHR). [1, 2, 3, 4] | `transport-staff` |

### Room Turnaround & Throughput

| # | `event_name` | Description (source) | Actor role |
|---|---|---|---|
| 11 | `bed_cleaning_required` | The moment the nurse clicks "discharged" in the EHR, the system automatically triggers an automated cleaning request to the EVS team, logging the room status as dirty. [1, 2] | *(system)* |
| 12 | `cleaning_priority_assigned` | EVS dispatch or the system software flags the room with a turnover priority code (e.g., STAT for immediate emergency department holds, NEXT, or NORMAL). [1] | `evs-staff` |
| 13 | `bed_cleaning_started` | An EVS staff member accepts the page and scans into the room's physical terminal or mobile app, updating the room's status to "in progress". [1] | `evs-staff` |
| 14 | `bed_cleaned` | EVS finishes terminal cleaning and chemical disinfection. They press a terminal button or submit an app notification, instantly flipping the digital bed board status to Clean & Available. [1, 2, 3, 4] | `evs-staff` |

> This final action completes the cycle, signaling the admissions team or the Emergency
> Department that the asset is ready for immediate patient placement. [1, 2]

---

## Exception event (added for this demo)

Not in the source document. Required to model the cancelled-and-restarted discharge.

| # | `event_name` | Description | Actor role |
|---|---|---|---|
| 15 | `billing_clearance_failed` | Finance could not clear the account — insurance adjudication was rejected or the patient's out-of-pocket responsibility could not be settled. The discharge is cancelled and the patient stays overnight. | `billing-mgr` |

---

## Event ordering

Happy path, in causal order:

```
pending_discharge_logged
  → attending_doctor_initiated          (status: initiated,  sets initiated_at)
  → dietary_tray_canceled
  → floor_nurse_clearance
  → pharma_return_started
  → pharma_return_received
  → billing_initiated_insurance
  → billing_insurance_received
  → billing_clearance | billing_clearance_failed
  → patient_displaced_discharged        (status: discharged, sets completed_at)
  ─────────── room is now empty ───────────
  → bed_cleaning_required
  → cleaning_priority_assigned
  → bed_cleaning_started
  → bed_cleaned
```

Two properties matter for the demo:

1. **EVS events occur after the patient has gone** and still carry the same `process_id`.
   `hims_discharge.status` stays `discharged` throughout — the row is final while its event
   stream continues.
2. **`billing_clearance_failed` cancels the discharge** (`status = cancelled`). The same
   `process_id` is restarted the next day, which updates an existing row and is what makes
   copy-on-write and merge-on-read behave differently.
