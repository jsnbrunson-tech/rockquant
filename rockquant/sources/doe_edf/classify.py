import re

def classify_edf_subtype(title: str) -> str:
    t = re.sub(r"\s+", " ", (title or "").lower()).strip()

    if re.search(r"\b(approves|approved|disburses|disbursed|releases|released|authorizes|authorized)\b.*\bdisbursement(s)?\b", t) \
       or re.search(r"\bloan\s+disbursement\b", t):
        return "DISBURSEMENT_APPROVED"

    if re.search(r"\b(early\s+voluntary\s+repayment|repayment|repaid|repays|paid\s+off|pays\s+off|prepayment|prepay)\b", t):
        return "REPAYMENT_RECEIVED"

    if re.search(r"\b(terminat(e|es|ed|ion)|withdraw(n|s|al)|cancel(s|led|lation)|rescind(s|ed|ing)|revoke(s|d|tion))\b", t):
        return "CONDITIONAL_COMMITMENT_TERMINATED"

    if re.search(r"\b(restructur(e|es|ed|ing)|amend(s|ed|ment)|modif(y|ies|ied|ication)|refinanc(e|es|ed|ing))\b", t):
        return "DEAL_RESTRUCTURED"

    if re.search(r"\b(close|closes|closed|finaliz(e|es|ed|ing)|financial\s+close)\b.*\b(loan\s+guarantee|guarantee)\b", t):
        return "DEAL_CLOSED_LOAN_GUARANTEE"

    if re.search(r"\b(close|closes|closed|finaliz(e|es|ed|ing)|financial\s+close)\b.*\bloan\b", t) and "guarantee" not in t:
        return "DEAL_CLOSED_LOAN"

    if re.search(r"\bannounce(s|d)\b.*\bconditional\s+commitment\b", t):
        return "CONDITIONAL_COMMITMENT_ANNOUNCED"

    if re.search(r"\b(offer(s|ed)?|issue(s|d)?|make(s|made)?|extend(s|ed)?|provide(s|d)?)\b.*\bconditional\s+commitment\b", t):
        return "CONDITIONAL_COMMITMENT_ISSUED"

    if re.search(r"\bannounce(s|d)\b.*\bloan\s+guarantee\b", t):
        return "DEAL_ANNOUNCED_GUARANTEE"

    if re.search(r"\bannounce(s|d)\b.*\bloan\b", t):
        return "DEAL_ANNOUNCED"

    if re.search(r"\bsolicitation\b|\brfp\b|request\s+for\s+proposals", t):
        return "SOLICITATION_ISSUED"

    if re.search(r"\bnotice\s+of\s+guidance\b|\bguidance\b", t):
        return "GUIDANCE_ISSUED"

    if re.search(r"\byear\s+in\s+review\b", t):
        return "REPORT_YEAR_IN_REVIEW"

    if re.search(r"\breport\b|\breports\b", t):
        return "REPORT_PUBLISHED"

    if re.search(r"\bdeployment\s+target\b", t) or re.search(r"\b\d+\s*gw\b.*\b(target|goal)\b", t):
        return "POLICY_TARGET_ANNOUNCED"

    if re.search(r"\bannounce(s|d)\b", t) and re.search(r"\$\s*[\d\.,]+\s*(billion|million|bn|b|m)\b", title or "", re.I):
        return "FUNDING_ANNOUNCED"

    if re.search(r"\bannounce(s|d)\b.*\bprojects?\b", t):
        return "PROJECTS_ANNOUNCED"

    if re.search(r"\bapplauds\b.*\bdecision\b|\bapplauds\b|\bwelcomes\b.*\bdecision\b", t):
        return "PROJECT_DECISION"

    return "EVENT_UNKNOWN"
