# User Stories — messaging_service

Derived from the platform system design (§2.5 `messaging_service`) and its inter-service
contracts (API-003, EVT-002, EVT-003, EVT-005, EVT-008, EVT-009). Actors: **member**
(any signed-in user), **muted member**, **non-member**, **author** (creator of a message),
**realtime gateway** (internal peer service).

---

## US-001: Request an image upload slot

**As a** member,
**I want to** request a presigned upload URL for an image before sending a message,
**So that** I can share pictures in chats without the backend proxying the bytes.

### Acceptance Criteria
- Given I declare `content_type` `image/png`, `image/jpeg`, `image/gif`, or `image/webp` and `size_bytes` ≤ 10 MB, when I request an attachment, then I receive an `attachment_id` and a presigned upload URL that expires in 15 minutes.
- Given I declare `size_bytes` greater than 10 MB, when I request an attachment, then I receive `413 image_too_large` with a message explaining the 10 MB limit.
- Given I declare a content type outside the four supported image types (e.g. `application/pdf`), when I request an attachment, then I receive `415 unsupported_image_type` with a message explaining the accepted types.
- Given I request an attachment while unauthenticated, then I receive `401`.

### Out of Scope
- Non-image files, video, and audio attachments.
- Inline image processing (resizing, thumbnails, virus scanning).

---

## US-002: Send a message in a text channel

**As a** member,
**I want to** post a message (up to 4,000 characters) in a text channel of a community I belong to,
**So that** the community can read what I wrote.

### Acceptance Criteria
- Given I am an active, non-muted member and the channel is a text channel of my community, when I send a message with content ≤ 4,000 chars, then it is created and returned with its id, author, timestamps, and reaction summary.
- Given I am not an active member of the community owning the channel (removed or banned), when I try to send a message, then I receive `403 not_a_member`.
- Given the channel does not exist (or is not a text channel), when I try to send, then I receive `404`.
- Given content longer than 4,000 characters, when I try to send, then I receive `422`.
- Given a mute is active on me in that community, when I try to send, then I receive `403 member_muted` including `muted_until` and an appeal pointer.

### Out of Scope
- Posting into voice channels.
- Any synchronous call to other services during send.

---

## US-003: Retry a send after a dropped connection without duplicating

**As a** member,
**I want to** re-send a message with the same `client_message_id` after my connection dropped,
**So that** my message appears exactly once even though my client retried.

### Acceptance Criteria
- Given I previously sent a message with `client_message_id` X in a container, when I send another message with the same X, the same author, and the same container, then the original message is returned unchanged and no duplicate row is created.
- Given two different members (or the same member in different containers) use the same `client_message_id`, when both send, then both messages are created independently.
- Given the first attempt failed before persisting (no stored message for X), when I retry with X, then a new message is created normally.

### Out of Scope
- Cross-container or cross-author deduplication of the same `client_message_id`.

---

## US-004: Reply in a thread

**As a** member,
**I want to** reply to a message to start or continue a thread,
**So that** side discussions stay out of the main channel flow.

### Acceptance Criteria
- Given a visible message in a container I can read, when I send a message with `thread_root_id` set to it, then my reply is stored with that root and appears under the thread.
- Given I request the thread of a root message, then I receive the root plus its replies (cursor-paged) with the reply count for the root.
- Given a message is deleted, when someone tries to reply to it, then the reply is rejected with `404`.

### Out of Scope
- Nested threads (a reply to a reply attaches to the same root).

---

## US-005: Read channel history with paging

**As a** member,
**I want to** page through a channel's history newest-first with a cursor,
**So that** I can scroll back through the conversation.

### Acceptance Criteria
- Given I am an active member of the community, when I request a channel's messages, then I receive the newest page first with an opaque cursor for the next (older) page and a `has_more` flag.
- Given I page until exhausted, when there are no older messages, then `next_cursor` is null and `has_more` is false.
- Given I am not an active member of that community, when I request history, then I receive `403 not_a_member`.
- Given deleted messages exist in history, when I read the page, then they appear as tombstones (no content) instead of disappearing, preserving reply/thread integrity.

### Out of Scope
- Full-text search of message history.

---

## US-006: Catch up after reconnect using an "after" cursor

**As a** member whose connection dropped,
**I want to** request the messages posted after a known point,
**So that** I can catch up exactly once without gaps or duplicates.

### Acceptance Criteria
- Given I hold an "after" cursor from my last seen message, when I request history with it, then I receive only messages created after that point in chronological order.
- Given no messages were posted after my cursor, when I catch up, then I receive an empty page.
- Given my reconnect is denied because I was removed from the community while offline, when I request catch-up, then I receive `403 not_a_member`.

### Out of Scope
- Pushing missed events through the WebSocket (the gateway replays nothing; catch-up is REST-only).

---

## US-007: Chat in a direct or group conversation

**As a** member,
**I want to** send and read messages in my direct and group private conversations,
**So that** private chats work like community channels.

### Acceptance Criteria
- Given I am a current participant of a conversation, when I send a message, then it is created and returned.
- Given I have left the conversation (or was never a participant), when I try to send or read, then I receive `403 not_a_member` — message content of a conversation I don't belong to is never revealed.
- Given the conversation does not exist, when I try to send, then I receive `404`.

### Out of Scope
- Creating or renaming conversations (owned by `social_service`).

---

## US-008: Edit my own message

**As a** member,
**I want to** edit a message I authored,
**So that** I can fix typos or clarify what I wrote.

### Acceptance Criteria
- Given I am the author of a non-deleted message, when I edit its content, then the message is updated, `edited_at` is set, and the new content is returned.
- Given I try to edit a message authored by someone else, when I attempt the edit, then I receive `403 not_author`.
- Given I try to edit a deleted message, when I attempt the edit, then I receive `409 message_deleted`.
- Given my edited content violates the community's automod rules, when I save the edit, then the same block/remove behavior as a new send applies.

### Out of Scope
- Editing by moderators or admins.

---

## US-009: Delete my own message

**As a** member,
**I want to** delete a message I authored,
**So that** I can remove something I posted by mistake.

### Acceptance Criteria
- Given I am the author, when I delete my message, then the content is removed for everyone and the message becomes a tombstone with deletion reason `author`.
- Given I try to delete someone else's message, when I attempt it, then I receive `403 not_author`.
- Given I delete a message twice (double-tap / retry), when the second request arrives, then I still receive `204` (delete is idempotent).

### Out of Scope
- Moderator deletion — that is automod-driven or a future moderation capability.

---

## US-010: React to a message with emoji

**As a** member,
**I want to** add and remove a Unicode emoji reaction on a message,
**So that** I can respond without writing a message.

### Acceptance Criteria
- Given a visible message in a container I can read, when I react with an emoji, then the aggregated reaction summary (emoji → count) is returned and my reaction is counted once.
- Given I already reacted with the same emoji, when I react again, then the summary is returned unchanged (adding is idempotent, no double count).
- Given I had reacted with an emoji, when I remove it, then the count decreases; removing a reaction I don't have leaves counts unchanged and still returns the summary.
- Given a deleted message, when I try to react, then I receive `404`.

### Out of Scope
- Custom or animated server-side emoji.

---

## US-011: Mention members in a message

**As a** member,
**I want to** mention other members of the container with `<@{user_id}>` tokens in my message,
**So that** they get notified (notifications are handled by the notification service).

### Acceptance Criteria
- Given I write a message containing `<@{user_id}>` tokens, when I send it, then each token is validated against the container's current membership and the resolved mention list is stored and included on the emitted message event.
- Given I reference a user who is not a current member of the container, when I send, then that token is ignored (dropped from the mention list) and the message still posts.
- Given a message mentions more members than the automod spam rule allows, when I send it, then the automod mention-flood rule fires exactly as configured.

### Out of Scope
- Sending notifications — `notification_service` consumes the message event.

---

## US-012: Access enforcement mirrors membership state

**As a** community member,
**I want to** my posting rights in channels to follow my current membership, mute, and removal state,
**So that** sanctions and membership changes take effect everywhere without a manual refresh.

### Acceptance Criteria
- Given a mute was applied to me, when I try to post in any channel of that community, then I receive `403 member_muted` with `muted_until` and how to appeal; reading history still works.
- Given my mute expired, when I try to post again, then the send succeeds.
- Given I was kicked or banned, when I try to post or read that community's channels, then I receive `403 not_a_member`.
- Given a private conversation I participate in, then my access is governed by the conversation's participant list, independent of any community membership.

### Out of Scope
- Issuing or lifting sanctions — `moderation_service` does that; this service only mirrors the outcome.

---

## US-013: Automatic filters block bad messages before posting

**As a** community admin,
**I want to** my community's automatic-filter rules (spam thresholds, banned words) to reject violating messages before they appear,
**So that** harmful content is never visible.

### Acceptance Criteria
- Given the community has a `block` rule for banned words and a member posts matching content, when the send arrives, then it is rejected with `422 blocked_by_automod` including the rule category.
- Given the spam rule limits messages per member per time window and a member exceeds it, when they send again, then the send is rejected with `422 blocked_by_automod` (category `spam`).
- Given a member holds a role that is exempt from automod, when they post rule-violating content, then the message posts normally.
- Given the community has automod disabled or no rule set, when members post, then nothing is filtered.
- Given a rule set update arrives for a community, when the next message is posted, then the newest rule-set version is applied.

### Out of Scope
- Configuring the rules — `moderation_service` owns configuration and publishes it.

---

## US-014: Automatic filters remove violating messages after posting

**As a** community admin,
**I want to** configure `remove` actions so borderline spam is posted briefly and then removed,
**So that** enforcement is visible-but-brief for borderline cases I choose.

### Acceptance Criteria
- Given a `remove` rule matches a member's message, when it is sent, then the message is accepted, then removed shortly after with deletion reason `automod`, and observers see the removal.
- Given a message was removed by automod, when anyone reads history, then it appears as a tombstone with reason `automod` (not `author`).
- Given the author of an automod-removed message, when I view it, then I still see it as deleted and cannot edit it.

### Out of Scope
- Notifying the author with a direct message about the removal.

---

## US-015: Attach an already-uploaded image to a message

**As a** member,
**I want to** reference an image I uploaded when sending a message,
**So that** recipients receive it with the message.

### Acceptance Criteria
- Given I uploaded an image and its object exists with a supported type and size within limits, when I send a message referencing its `attachment_id`, then the attachment is bound to my message and recipients get a short-lived download URL.
- Given the referenced upload never happened or is not mine, when I send, then the send fails with `422 attachment_invalid`.
- Given the uploaded object exceeds 10 MB or its type is unsupported, when I send referencing it, then I receive `413 image_too_large` or `415 unsupported_image_type` respectively.
- Given I uploaded an image but never send it, when 24 hours pass, then the upload and its stored object are garbage-collected and can no longer be attached.

### Out of Scope
- Re-using one attachment in multiple messages.

---

## US-016: Answer the realtime gateway's subscription-scope question

**As the** platform's realtime gateway,
**I want to** ask which communities and private conversations a connecting user may receive live events for,
**So that** the WebSocket only pushes events the user may see.

### Acceptance Criteria
- Given a valid service-token call naming a user, when I answer, then I return the communities where the user is an active member (muted included, removed/banned excluded) and the conversations where they are a current participant, with a generation timestamp.
- Given an unknown user id, when I answer, then I return `200` with empty scope lists (not an error).
- Given a call without a valid internal service token, then I receive `401`.

### Out of Scope
- Keeping the gateway's scopes current after connect (the gateway maintains them from membership and conversation events itself).

---

## US-017: Publish every message change as an event

**As the** notification and realtime services,
**I want to** receive an event for every message creation, edit, deletion, and reaction change,
**So that** live views and notifications stay in sync without polling.

### Acceptance Criteria
- Given a message is created, when it is accepted (automod-blocked sends are rejected and never persisted), then a `created` event with the full message snapshot (content, attachments with fresh download URLs, mentions, reactions) is published.
- Given a message is edited, deleted, or its reactions change, when the change commits, then the corresponding `edited` / `deleted` / `reactions_changed` event with the message snapshot is published.
- Given a consumer receives the same event twice (at-least-once delivery), when it processes the duplicate, then it applies nothing new because the event carries a unique id and a per-message version.

### Out of Scope
- Event delivery guarantees beyond at-least-once with consumer-side dedupe.
