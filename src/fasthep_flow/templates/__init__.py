from __future__ import annotations

import jinja2


def template_environment() -> jinja2.Environment:
    """Return a Jinja2 environment for rendering templates."""
    return jinja2.Environment(
        loader=jinja2.PackageLoader("fasthep_flow", "templates"),
        autoescape=jinja2.select_autoescape(["html", "py"]),
    )
