# Agile project management using GitHub Projects

> Converted from `Agile Project Managment using GitHub Projects.docx`. This first
> pass is a faithful reformatting — the structure, claims and code are the
> source document's. A review pass follows; see [Review notes](#review-notes) at
> the end for what has been checked against a live repository and what has not.

**GitHub Projects (v2)** provides a flexible system for running Agile development
natively alongside the source code. GitHub offers REST API endpoints for project
items and fields, but full programmatic manipulation of Projects v2 still relies
heavily on the **GraphQL API**.

---

## Part 1: Mapping Agile primitives to GitHub

GitHub is built around Issues, so Agile concepts map onto standard GitHub
features plus **custom fields**.

```
┌────────────────────────────────────────┐
│                 EPIC                   │  Parent issue (using sub-issues), or
└───────────────────┬────────────────────┘  an "Epic" issue type / label
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
┌──────────────┐        ┌──────────────┐
│  USER STORY  │        │  USER STORY  │    Standard issues linked to the epic
└───────┬──────┘        └───────┬──────┘
        │                       │
        ▼                       ▼
┌──────────────┐        ┌──────────────┐
│    TASKS     │        │    TASKS     │    Markdown task lists, or nested
└──────────────┘        └──────────────┘    sub-issues
```

### 1. Managing epics and user stories

A hierarchy can be built from GitHub's **sub-issues** and **issue types**:

- **The epic.** Create a standard issue and assign it an `Epic` issue type, or
  apply an epic label. Define the overall feature scope and acceptance criteria
  in the body.
- **The stories.** Use **sub-issues** to break the epic down. Each sub-issue
  links back to its parent automatically, keeping the tree traceable.

### 2. Managing sprints

Sprints use the project's native **iteration** custom field:

1. In project settings, add a new custom field of type **Iteration**.
2. Name it `Sprints`.
3. Set the duration (e.g. two weeks). GitHub generates consecutive intervals
   (*Sprint 1: Oct 1 – Oct 15*) and rolls them over automatically.

### 3. Setting up Agile views

Create several views (tabs) inside the project:

| View | Layout | Configuration |
|---|---|---|
| **Product backlog** | Table | Sorted by priority; filtered to items where the sprint field is empty |
| **Active sprint kanban** | Board | Grouped by `Status` (Todo, In Progress, Review, Done), filtered by `@current` iteration |
| **Roadmap / epic** | Roadmap | Grouped or filtered by epic, to visualise multi-sprint timelines |

---

## Part 2: Working with the APIs

GitHub offers a hybrid approach for Projects v2.

### Is there a REST API?

**Yes** — there are REST endpoints for project items and fields, so items can be
listed, added and fetched.

**For anything complex, use GraphQL.** The Projects v2 architecture is
GraphQL-first. Simple additions can go through REST, but updating a custom field
— changing the assigned sprint iteration, say — generally requires a GraphQL
mutation.

### 1. Adding an issue to a project (REST)

```bash
curl -X POST \
  -H "Authorization: Bearer $GITHUB_TOKEN" \
  -H "Accept: application/vnd.github+json" \
  https://api.github.com/orgs/ORG/projectsV2/PROJECT_NUMBER/items \
  -d '{"content_id": 12345678, "content_type": "Issue"}'
```

`content_id` is the global node ID of the issue or pull request being attached.

### 2. Querying project data (GraphQL)

Query the `projectV2` object to pull items together with their custom field
values:

```graphql
query {
  organization(login: "YOUR_ORG_NAME") {
    projectV2(number: YOUR_PROJECT_NUMBER) {
      id
      title
      items(first: 20) {
        nodes {
          id
          type
          content {
            ... on Issue {
              title
              number
              repository { name }
            }
          }
          fieldValues(first: 10) {
            nodes {
              ... on ProjectV2ItemFieldSingleSelectValue {
                name
                field { ... on ProjectV2Field { name } }
              }
              ... on ProjectV2ItemFieldIterationValue {
                title
                duration
                field { ... on ProjectV2IterationField { name } }
              }
            }
          }
        }
      }
    }
  }
}
```

### 3. Moving an item to a sprint (GraphQL mutation)

```graphql
mutation {
  updateProjectV2ItemFieldValue(
    input: {
      projectId: "PROJECT_NODE_ID"
      itemId: "ITEM_NODE_ID"
      fieldId: "SPRINT_FIELD_NODE_ID"
      value: { iterationId: "SPECIFIC_SPRINT_ITERATION_NODE_ID" }
    }
  ) {
    projectV2Item { id }
  }
}
```

---

## Automating with GitHub Actions

The most robust route in CI is the **`gh` CLI**, pre-installed on every GitHub
Actions runner. It authenticates from the token in the environment and talks to
the Projects v2 GraphQL endpoint without external Python dependencies.

The example below moves an issue into the current active sprint.

### Step 1: the automation script

`.github/scripts/agile_move_to_sprint.py`:

```python
import json
import os
import subprocess
import sys

PROJECT_NUMBER = os.environ.get("PROJECT_NUMBER")
ORGANIZATION = os.environ.get("ORGANIZATION_NAME")
ISSUE_NODE_ID = os.environ.get("ISSUE_NODE_ID")

if not all([PROJECT_NUMBER, ORGANIZATION, ISSUE_NODE_ID]):
    print("Missing required environment variables.")
    sys.exit(1)


def run_gh_graphql(query, variables=None):
    """Execute a GraphQL query through the GitHub CLI."""
    cmd = ["gh", "api", "graphql", "-f", f"query={query}"]
    for key, val in (variables or {}).items():
        cmd.extend(["-F", f"{key}={val}"])
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"CLI error: {result.stderr}")
        sys.exit(1)
    return json.loads(result.stdout)


# 1. Project ID, the `Sprints` field ID, and the current iteration.
FETCH_META = """
query($org: String!, $num: Int!) {
  organization(login: $org) {
    projectV2(number: $num) {
      id
      field(name: "Sprints") {
        ... on ProjectV2IterationField {
          id
          configuration {
            activeIterations { id title }
          }
        }
      }
    }
  }
}"""

print("Fetching project and sprint configuration...")
meta = run_gh_graphql(FETCH_META, {"org": ORGANIZATION, "num": PROJECT_NUMBER})

project = meta["data"]["organization"]["projectV2"]
project_id = project["id"]
sprint_field = project.get("field")

if not sprint_field:
    print("No custom project field named 'Sprints' — check the casing.")
    sys.exit(1)

active = sprint_field["configuration"]["activeIterations"]
if not active:
    print("No active sprint iteration right now.")
    sys.exit(0)

current_sprint_id = active[0]["id"]
current_sprint_title = active[0]["title"]

# 2. Add the issue to the board. An item must exist before its fields can be set.
ADD_ITEM = """
mutation($projId: ID!, $contentId: ID!) {
  addProjectV2ItemById(input: {projectId: $projId, contentId: $contentId}) {
    item { id }
  }
}"""

print(f"Adding or locating issue {ISSUE_NODE_ID} in the project...")
item = run_gh_graphql(ADD_ITEM, {"projId": project_id, "contentId": ISSUE_NODE_ID})
item_id = item["data"]["addProjectV2ItemById"]["item"]["id"]

# 3. Set the sprint field to the active iteration.
UPDATE_SPRINT = """
mutation($projId: ID!, $itemId: ID!, $fieldId: ID!, $sprintId: String!) {
  updateProjectV2ItemFieldValue(
    input: {
      projectId: $projId
      itemId: $itemId
      fieldId: $fieldId
      value: { iterationId: $sprintId }
    }
  ) {
    projectV2Item { id }
  }
}"""

print(f"Moving item to active sprint: {current_sprint_title}")
run_gh_graphql(
    UPDATE_SPRINT,
    {
        "projId": project_id,
        "itemId": item_id,
        "fieldId": sprint_field["id"],
        "sprintId": current_sprint_id,
    },
)
print("Done.")
```

### Step 2: the workflow

`.github/workflows/agile-sprint-automation.yml`:

> **Authentication.** The default `GITHUB_TOKEN` does not carry access to
> organization-level projects. Use a personal access token with `project` scope,
> or a GitHub App token, stored as a repository secret.

```yaml
name: Agile Project Automation

on:
  issues:
    types: [opened, demilestoned]

jobs:
  assign-to-sprint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Assign to the active sprint
        env:
          PROJECT_NUMBER: 5
          ORGANIZATION_NAME: "your-github-org-name"
          ISSUE_NODE_ID: ${{ github.event.issue.node_id }}
          GH_TOKEN: ${{ secrets.PROJECT_AUTOMATION_TOKEN }}
        run: python .github/scripts/agile_move_to_sprint.py
```

### Step 3: open questions

1. **Which trigger** should move an item — a `status: in-progress` label being
   added, or a pull request being opened?
2. Should the script also calculate and update **story points**?

---

## Review notes

Recorded during conversion; **not yet resolved**. The review pass addresses
these.

- The source's REST example posted to a bare `https://github.com` with no path.
  A plausible endpoint has been substituted and is **unverified**.
- The workflow's Python setup step read `python-python:`, corrected here to
  `python-version:`.
- The Python listing had its newlines stripped by the document conversion
  (`import osimport sysimport json`) and has been reconstructed. The logic is the
  source's; it has not been executed.
- Every GraphQL example is rooted at `organization(login:)`. A **user-owned**
  repository needs `user(login:)`, which matters for this repository.
- **Issue types** are presented as generally available. They are an
  organization-level feature.
- The workflow triggers on `demilestoned` and, in the source, on `project_card`
  — the latter belongs to the classic projects API and does not fire for
  Projects v2.
- All inline citations from the source (largely video and blog links) were
  dropped in favour of the structure itself; the authoritative reference is
  GitHub's own Projects documentation.
