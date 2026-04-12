from flask import Blueprint, current_app, render_template

main = Blueprint("main", __name__)


@main.get("/")
def index() -> str:
    return render_template(
        "index.html",
        td=current_app.extensions["td"],
    )
