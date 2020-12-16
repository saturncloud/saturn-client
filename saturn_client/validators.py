import re
from typing import Any, Dict, List, Optional


def validate_name(name, required=False):
    name_re = "^[a-z0-9-]{2,120}$"
    errors: List[str] = []
    if required and name is None:
        errors.append("Name is required.")
    elif name is None:
        pass
    elif not re.match(name_re, name.lower().replace("_", "-")):
        errors.append("Invalid project name. Name must have no spaces and only contain letters and numbers.")
    return errors

def validate_description(description):
    errors:  List[str] = []
    elif len(description) > 255:
        errors.append("Description is too long. It must be less than 255 characters.")
    return errors
