# Elisa test assignment - Helena Rivas

We'll be developing a test assignment for a job application at Elisa. The task
is described in @Docs/task-description.txt.

Read the task description and familiarize yourself with it.
Before doing anything, let's setup some rules for how we'll work.

## Development flow

We'll use a github project to define and track our work in a Kanban style. You
can find use the `gh` tool for all aspects of this, from creating issues, to
moving them on the board, to writing PRs and doing reviews, etc. We'll also
setup a CI pipelne. The current repo points to the correct GH remote, and the
project itself can be found in https://github.com/users/hrhelena/projects/1

You will use agents for the process:

 - A product owner agent, who will map the requirements in the task descriptions
   to actual tickets in the project. This agent will interact with me as needed
   to clarify questions and document decisions. Architectural decisions will be
   documented in the @Docs/ADR folder. Keep the ADR documents consistent
   (markdown, with a title, author, summary, and rationale sections) The tickets
   will contain detailed specs for the features being implemented, as well as
   Gherkin specs for the expected behavior. The coding agent (cf below) will use
   these as a base for planning its work, and developing both code and suitable
   tests.

 - A coding agent, who will be in charge of taking tickets from the backlog, and
   working on them (updating their status as it goes, too). The coding agent
   will use Golang for development, and ensure that standard Golang best
   practices are followed. Linting and formatting, for example, should be
   checked with precommit hooks. Both unit and integration tests will be
   developed for each feature as needed. Whenever an architectural decision
   needs to be made, the coding agent will consult me, and document the
   resulting decision in an ADR. Once a ticket has been worked on, the coding
   agent will open a well-written PR on the repo, and pass it to the review
   agent (cf below). The review agent will then review the PR and get back to
   the coding agent with requested changes as needed. This cycle will continue
   until all observed issues in the PR have been solved. Should there be a need
   for arbitration, the review agent will ask me for instructions.

 - A review agent, who will dilligently review PRs and focus on ensuring the
   code's correctness.

You will create suitable agents and skills (e.g. for PR descriptions / review)
as needed, locally within this project. Agents will use worktree isolation by
default, so they can work in parallel.

## Infrastructure

The task description mentions a `cloud native solution`, so we shall interpret
this as needing to ensure that our solution can be deployed to something like
EKS or the GCP kubernetes engine. You will setup the required configuration
management to ensure that all operations aspects of the project are properly
handled. I've got docker installed, and also `kind` for doing local k8s
deployments for testing.

## Starting Architectural decisions:

- The referred dataset is already downloaded and uncompressed in the @Dataset
directory. You are free to pick the most suitable libraries / tools to parse the
information in it (it's in maildir format), to implement the data ingestion.
Given that the dataset itself is not overly large (2.3GB or so), the choice of
database can be reduced to either sqlite or Postgres. You will do an analysis of
pros/cons for each, and discuss it with me (I'd probably choose Postgres, but I
want a proper analysis done). Likewise, analyse the requirements for deciding
whether full-text indexing is needed or not. Since the data comes from an
external source, it may contain invalid, or poorly formatted entries. Plan on
dealing with these, instead of failing when you find invalid entries.

- The task spec is ambiguous wrt the way the dataset should be ingested. Their
  `start` endpoint description talks about ingesting and tracking indexing, not
  downloading the dataset. We will add a `fetch` endpoint to handle the download
  and decompression of the data, if needeed. (btw, the Dataset directory should
  be .gitignored). Write an ADR to reflect this.

- Keep the design flexible to potential extensions, and suggest a few possible
  ones in the README (e.g., full-text indexing, other data sources, more complex
  data mining / querying). 

- Since this is a "toy project" assignment, we will not do AAA / IAM-related
  work. However, the architecture should be able to be easily extended to
  include that. The decision not to implement these functionalities needs to be
  recorded in an ADR.

If you have questions for getting started, ask me now. Once everything is clear,
create an action plan for implementing this project, and start working on the
different aspects (agents, defining specs / tickets from the description, etc.)


