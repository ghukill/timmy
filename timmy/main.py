from flask import Blueprint, render_template
from timmy.dataset import get_app_dataset

main = Blueprint("main", __name__)


@main.get("/")
def index() -> str:
    td = get_app_dataset()
    return render_template(
        "index.html",
        td=td,
    )
