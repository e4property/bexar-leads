# Outreach Scripts — Foreclosure (NOF) live, APPT pending

Sent via Jarvis (GHL) automation, signed as Xavi throughout — Jarvis sends
the texts, but Xavi is the real person who follows up, so keeping that
identity consistent isn't a false persona.

Tone rules (apply to all messages, both lead types):
- Sound like a regular local person, not an investor — never use the
  word "investor," avoid value-prop/tagline phrasing ("I help homeowners
  find a way out...") — that reads as ad copy, not a person talking.
- Lead's name: first name only in message 1, no name at all after that.
- Sender's name (Xavi) can and should appear in later messages — that's
  not the same rule, it's what keeps the identity consistent/trustworthy.
- Every text needs the opt-out line — Texas SB140 (2025) extends
  telephone-solicitation rules to texts specifically; exposure is
  $500/violation, $1,500 if willful, uncapped aggregate. Scrub against
  the National DNC list before texting.

## Foreclosure (NOF) — live sequence, finalized 2026-08-05

Revised after reviewing the actual live script and researching what reads
as templated/investor-coded vs. genuinely human. Flagged and cut: value-
proposition taglines ("find a way out... on their terms, not the bank's"),
generic testimonial claims ("I've helped homeowners... just like this
one"), and overt self-vouching ("I'm local, I'm real, and I'm not going
anywhere" — can paradoxically read as defensive/scam-like).

| Day | Message |
|---|---|
| 0 | SMS 1 |
| 0-1 | Ringless voicemail (existing, not reviewed — no transcript available) |
| 1-2 | SMS 2 |
| 4 | SMS 3 |
| 7 | SMS 4 (final) |

**SMS 1 (Day 0):**
> Hi [First], this is Xavi — I noticed your property at [Address] has a
> foreclosure filing. Wanted to reach out directly before it gets any
> closer to the auction date. No pressure, just wanted to see if you're
> doing okay and if there's anything I can do to help. Reply STOP to
> opt out.

**SMS 2 (Day 1-2):**
> Hey, Xavi again. Following up in case my last text got buried. No
> pressure — just let me know if you want to talk through what's going
> on or if there's anything I can help with. Reply STOP to opt out.

**SMS 3 (Day 4):**
> Checking in — the auction date doesn't wait, so if you haven't sorted
> something out yet, I'm still around. Doesn't have to be a call either,
> a text back works just as well if that's easier. Reply STOP to opt out.

**SMS 4 (Day 7, final):**
> This is my last reach out for now, and I mean that with respect, not
> as a sales tactic. If the timing never works out, I genuinely hope
> things turn around for you. But if you ever want to talk through
> options before the auction, I'm one text away. — Xavi. Reply STOP to
> opt out.

**A/B alternative for SMS 1**, if ever worth testing — shorter,
question-led, defers all detail to SMS 2 (some sources rate this higher-
reply, though foreclosure leads specifically may respond better to full
transparency upfront since they're already targeted by predatory/scam
outreach — untested which wins for this audience):
> Hi [First], this is Xavi. Are you still the owner of [Address]? Saw a
> foreclosure filing come through and wanted to check in. Reply STOP to
> opt out.

## Reply-handling (once someone responds — this is where most wins happen)

Goal: get a phone call if they'll take it, but keep moving forward over
text if that's their preference — collect enough info (condition,
timeline, why selling, rough price expectation) to make an offer once
the numbers are run.

- **Any positive/curious reply** → *"Glad you reached out! Easiest way to
  figure out if I can help is a quick call — what's a good time today or
  tomorrow? If a call's not your thing, happy to just text back and
  forth too."*
- **They reply but prefer text** → *"No problem, text works. Mind
  sharing a bit about the property — how many beds/baths, and roughly
  what kind of shape is it in?"* → then → *"Got it, thanks. What's your
  timeline like — are you looking to move quickly or just exploring
  options?"* → then → *"That's helpful. Do you have a number in mind for
  what you'd want, or want me to run some numbers and get back to you
  with an offer?"*
- **"How much are you offering"** (before enough info is gathered) →
  *"I want to give you a real number, not a guess — mind if I ask a
  couple quick things about the property first so it's accurate?"*
- **Vague/one-word reply ("ok", "who is this")** → *"Sorry — I'm Xavi, I
  reached out about your property on [Street]. Just seeing if you'd be
  open to chatting, no pressure."*
- **"I already have a loan modification in progress"** → *"That's great
  — I hope it works out. Loan mods can take 3-6 months though. Would it
  be okay if I check back in a few weeks, just in case?"*
- **"How did you get my information?"** → *"It becomes public record
  when a foreclosure filing happens — I reach out because sometimes I
  can help."*
- **"Not interested"** → *"Totally understand — are you facing any
  challenges with the property, or is everything handled?"* (gives room
  to correct you if it's already resolved, rather than assuming
  disinterest).

## Pre-Foreclosure (APPT) — NOT YET SENT, pending finalization

No SMS has gone out to APPT leads yet (as of 2026-08-05). Do not reuse
the NOF sequence as-is — APPT has no sale date, and the homeowner is
often still in denial (unopened certified mail, avoiding lender calls),
so pushing urgency here tends to backfire. Draft below is a starting
point only, not reviewed/finalized the way NOF is:

**SMS 1 (Day 0) — draft, needs the same review pass as NOF got:**
> Hi [First], this is Xavi. I help homeowners who might be dealing with
> mortgage challenges — no pressure, just wanted to see if you'd like to
> talk through your options. Reply STOP to opt out.

Before sending any of these: pull the actual current APPT workflow from
Jarvis if one exists, and run it through the same critique pass the NOF
script got (cut tagline/testimonial-style phrasing, keep it short and
person-to-person).

## Also on the to-do list

Update the AI Agent persona/instructions in Jarvis (Automation → AI
Agents) to match this script's tone once finalized — that's what governs
how Jarvis's AI responds in conversations, and it should sound like the
same person as these texts.

Sources: flipmantis.com/resources/pre-foreclosure-scripts,
realestateskills.com, batchdialer.com, landvoice.com, textdrip.com,
dealmachine.com (TX SB140 compliance).
