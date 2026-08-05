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

## AI Agent qualification flow — "Xavi - Bexar Bot," live in Jarvis

Once someone replies, "Xavi - Bexar Bot" (Automation → AI Agents →
Conversation AI, primary agent, SMS channel) takes over and runs this
flow automatically — this is where most of the actual reply-handling
happens now, not a manual script. Updated live 2026-08-06 after
reviewing what was there (rigid fixed-order questions, no guidance for
price-asks) and researching what top wholesalers/AI-bot platforms
recommend instead.

Current live instructions (Trigger 2, "Bexar Foreclosure Knowledge
Base"):

> Use this knowledge base when qualifying motivated sellers in
> foreclosure. Cover, in roughly this order but adapt if they've already
> shared something — don't force a rigid sequence: 1) Property condition
> (roof, HVAC, water heater, recent updates) 2) Mortgage payoff amount
> and any other liens, plus what they'd like to walk away with 3) Months
> behind on payments 4) Monthly payment amount 5) Who is on title 6) Book
> appointment. Ask one question at a time, keep messages short. Accept
> short answers and move on. Never ask "do you still owe on the
> mortgage" — assume they owe and go straight to payoff amount. If asked
> for a specific price or offer, don't state one — ask "what number did
> you have in mind?" instead. Making an actual offer is Xavi's job at the
> appointment.

What changed and why:
- **No longer a rigid fixed-order script.** Practitioner consensus is
  that forcing every question in a strict sequence when the seller
  volunteers info out of order reads robotic and can dead-end the
  conversation. Bot now adapts instead.
- **Property condition** broken into specifics (roof, HVAC, water
  heater, recent updates) instead of one open question — reads less
  like an interrogation.
- **Payoff + liens combined** with "what would you like to walk away
  with" — doubles as a motivation-gauging question.
- **Price-ask handling added.** Researched whether the bot should give a
  soft ballpark instead of fully deferring — no source recommends
  quoting a range before condition/payoff/liens are known, even among
  wholesalers who eventually give a verbal ballpark (that only happens
  after a callback + desktop underwriting, never on first contact). The
  well-supported middle ground instead: reverse the question — *"what
  number did you have in mind?"* — keeps the bot from ever committing to
  a number (a bad/hallucinated price can kill a deal) while still
  engaging rather than stonewalling.

## Manual talking points — for objections outside the bot's flow

The bot's flow above covers qualification. These cover objections/
scenarios it doesn't have explicit steps for — useful if Xavi jumps into
a conversation personally, or worth adding to the bot's instructions
later if they come up often:

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
- **Vague/one-word reply ("ok", "who is this")** → *"Sorry — I'm Xavi, I
  reached out about your property on [Street]. Just seeing if you'd be
  open to chatting, no pressure."*

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

**Open question:** "Xavi - Bexar Bot" is the only *primary* Conversation
AI agent (only the primary agent replies to inbound messages — a second
bot, "Xavi - Bexar VBP Bot," exists for VBP/CE leads specifically). Its
Trigger 2 knowledge base is scoped to foreclosure qualification. Unclear
yet whether APPT replies get handled by this same bot with the wrong
(foreclosure-specific) flow, or fall through with no structured
handling at all — check this before sending any APPT texts.

Sources: flipmantis.com/resources/pre-foreclosure-scripts,
realestateskills.com, batchdialer.com, landvoice.com, textdrip.com,
dealmachine.com (TX SB140 compliance and seller-objection scripts),
crushingrei.com (qualification question phrasing), biggerpockets.com
(verbal-ballpark-offer practice), getperspective.ai and noem.ai (AI
qualification bot conversation design).
