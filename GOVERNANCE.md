# Governance

## Decision Making

- **Day-to-day**: lazy consensus. If no objection within 72 hours, the proposal is approved.
- **Releases**: formal vote. Requires 3 binding +1 votes from PMC members, with a minimum 72-hour voting window.
- **New committers**: vote on the private mailing list.

## Roles

### Contributor

Anyone who files issues, submits PRs, or participates in discussions.

### Committer

A contributor who has been granted write access to the repository. Committers can merge PRs, review code, and participate in release testing.

**How to become a committer** (demonstrated over 6+ months):
- 10+ merged PRs of quality
- Active code review participation
- Mailing list or discussion participation
- Helping other contributors

### PMC Member

A committer who participates in project governance decisions.

**How to become a PMC member** (everything above, plus):
- Participating in release votes
- Mentoring new contributors
- Demonstrated judgment in project direction

## Voting

Votes follow the Apache voting process:

- **+1**: Yes, approve
- **0**: No opinion
- **-1**: Veto (must include technical justification)

A -1 vote on a release blocks it until the concern is resolved.

## Lazy Consensus

Most decisions use lazy consensus: a proposal is shared, and if no one objects within 72 hours, it is considered approved. This keeps the project moving without requiring formal votes for routine work.

## Communication

| Channel | Purpose |
|---------|---------|
| GitHub Discussions | Questions, ideas, show-and-tell |
| GitHub Issues | Bug reports, feature requests |
| Slack | Real-time chat |
| dev@ mailing list | Formal decisions (when under Apache governance) |
