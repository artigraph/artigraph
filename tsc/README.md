# TSC Materials for Artigraph

This directory contains the meeting notes, process documentations, and other materials related to this project.

## Project Intake Checklist

This is a checklist for TSC's to review as part of the intake process. The TSC should review this entire list during the kickoff meeting. For anything outstanding, create an issue to track and link to it in the list

### Prior to Project Contribution Proposal

- [ ] Project license identified and exists in root directory of all repos (named LICENSE)
- [ ] Any third-party components/dependencies included are listed along with thier licenses (example template at [THIRD_PARTY.md](../THIRD_PARTY.md))
- [ ] Code scan completed and any recommendations remedied.
- [ ] README.md file exists (template started at [README.md](../README.md))
- [ ] Governance defined, outlining community roles and how decisions are made (starting point at [GOVERNANCE.md](../GOVERNANCE.md) if needed).
- [ ] Contribution Policy defined ([CONTRIBUTING](../CONTRIBUTING.md))
- [ ] Code of Conduct defined (default is at [CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md) - if using a different code of conduct please contact [LF AI and Data Staff](mailto:operations@lfaidata.foundation)).
- [ ] Release methodology defined ([RELEASE.md](../RELEASE.md))
- [ ] Release/Testing process defined ([RELEASE.md](../RELEASE.md))
- [ ] Committers/Maintainers defined in the project ([COMMITTERS](COMMITTERS.csv) or MAINTAINERS file - recommended to be in a CSV format for machine parsing)
- [ ] Project support policies defined ([SUPPORT](../SUPPORT.md))

### Project Proposal

- [ ] Project Charter completed and approved ([CHARTER](CHARTER.pdf))
- [ ] Trademarks/mark ownership rights (complete [LF Projects - Form of Trademark and Account Assignment](lf_projects_trademark_assignment.md))
- [ ] Submit a completed Project Contribution Proposal via a GitHub pull request to https://github.com/lfai/proposing-projects/tree/master/proposals.
- [ ] Presentation scheduled on the TAC calendar.
- [ ] Prepare a presentation on the project for the TAC meeting.

### Post Acceptance

- Project assets
  - [ ] Domain name (create [service desk request] to setup/transfer)
	- [ ] Social media accounts (create [service desk request] to setup/transfer)
	- [ ] Logo(s) (create [service desk request] to create]; will be added to [artwork repo] in SVG and PNG format and color/black/white)
- Infrastructure
  - [ ] Source Control (Github, GitLab, something else ) and LF AI and Data Staff is an administrator.
    - [ ] Developer Certificate of Origin past commit signoff done and DCO Probot enabled.
  - [ ] Issue/feature tracker (JIRA, GitHub issues) and LF AI and Data Staff is an administrator.
  - Collaboration tools
    - [ ] Mailing lists - one of:
      - [ ] Create new list(s) (default is -discussion@ and -private@ - create [service desk request] to provision)
      - [ ] Move to groups.io (create [service desk request] to setup/transfer)
    - [ ] Establish project calendar on groups.io (refer to [tac guidelines])
    - [ ] Slack or IRC (create [service desk request] to setup Slack project channel)
  - [ ] Website (refer to [tac guidelines])
  - [ ] CI/build environment
 	- [ ] Add project to [LFX Insights] (create [service desk request] to trigger)
- TSC Formation
  - [ ] TSC members identified, added to [GOVERNANCE.md].
  - [ ] First TSC meeting held ([agenda](meetings))
  - [ ] TSC Chairperson identified and added to [GOVERNANCE.md]
  - [ ] TSC meeting cadence set and added to project calendar (https://lists.lfaidata.foundation/calendar)
  - [ ] TSC meeting minutes saved under (meetings).
- Outreach
  - [ ] New project announcement done (create [service desk request] to trigger)
  - [ ] Project added to LF AI and Data website and LF AI and Data landscape

### Stage Requirements

#### Incubation Stage Requirements

- [ ] CII Badge achieved (apply at https://bestpractices.coreinfrastructure.org/en)
- [ ] Have at least two organizations actively contributing to the project.
- [ ] Have a defined Technical Steering Committee (TSC) with a chairperson identified, with open and transparent communication.
- [ ] Have a sponsor who is an existing LF AI & Data member. Alternatively, a new organization would join LF AI & Data and sponsor the project’s incubation application.
- [ ] Have at least 300 stars on GitHub; this is an existing requirement for a project to be listed on the LF AI & Data landscape.
- [ ] Submit a request for Incubation stage review by the TAC.
- [ ] Presentation scheduled on the TAC calendar.
- [ ] Prepare a presentation on the project for the TAC meeting.
- [ ] Affirmative vote of the TAC
- [ ] Affirmative vote of the Governing Board.

#### Graduation Stage Requirements

- [ ] Have a healthy number of code contributions coming from at least five organizations.
- [ ] Have reached a minimum of 1000 stars on GitHub.
- [ ] Have achieved and maintained a Core Infrastructure Initiative Best Practices Gold Badge.
- [ ] Have demonstrated a substantial ongoing flow of commits and merged contributions for the past 12 months.
- [ ] Have completed at least one collaboration with another LF AI & Data hosted project
- [ ] Have a technical lead appointed for representation of the project on the LF AI & Data Technical Advisory Council.
- [ ] Submit a request for Incubation stage review by the TAC.
- [ ] Presentation scheduled on the TAC calendar.
- [ ] Prepare a presentation on the project for the TAC meeting.
- [ ] Affirmative vote of two-thirds of the TAC.
- [ ] Affirmative vote of the Governing Board.


[artwork repo]: https://artwork.lfaidata.foundation
[service desk request]: https://github.com/lfai/foundation/issues/new/choose
[tac guidelines]: https://github.com/lfai/tac
[GOVERNANCE.md]: ../GOVERNANCE.md
[LFX Insights]: https://insights.lfx.linuxfoundation.org/projects/lfai-f
