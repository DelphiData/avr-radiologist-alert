# AVR Radiologist Alert

Automated hourly monitor for https://avrteleris.com/AVR/Index.aspx that:
- Logs in with AVR credentials
- Parses the study table
- Counts CT/MR studies in 60/90/120-minute buckets
- Sends SMS alerts when total in those buckets >= 20
- Runs only during configured windows (America/New_York):
  - Mon–Fri: 6pm–11:59pm
  - Sat: 4am–11:59pm
  - Sun: 12:00am–9:00pm

Artifacts (screenshot/HTML) are stored in `docs/` and `docs/status.json` is updated for an optional dashboard (GitHub Pages).

## Setup

1) Add GitHub Secrets (Settings → Secrets and variables → Actions → New repository secret):
   - `AVR_USERNAME` (e.g., Corbin2)
   - `AVR_PASSWORD` (e.g., Baptist22)
   - `TWILIO_ACCOUNT_SID`
   - `TWILIO_AUTH_TOKEN`
   - `TWILIO_FROM_NUMBER` (E.164, e.g., +18595550123)

   Optional free fallback (email-to-SMS):
   - `SMTP_HOST`, `SMTP_PORT` (e.g., 587), `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_FROM`

2) Verify/edit contacts in `contacts.yml`.

3) Adjust any selectors in `config.yml` after the first run if needed. The first run saves:
   - `docs/last_screenshot.png`
   - `docs/last_page.html`
   - `docs/color_samples.json` (to help define `color_map` if row colors represent time buckets)

4) Workflow
   - Runs hourly (UTC), script enforces allowed windows in local time.
   - You can trigger manually via “Run workflow”.

## Notes
- Twilio trial accounts can only message verified numbers and add a “trial” banner. For production delivery, use a paid number (`TWILIO_FROM_NUMBER`).
- If Twilio env vars are not set or sending fails, and SMTP settings exist, it will try email-to-SMS (no direct SMS fees).
