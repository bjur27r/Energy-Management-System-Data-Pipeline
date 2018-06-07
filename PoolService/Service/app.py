#Bloque importacion de librerias
import Service.prediction


from flask import Blueprint
main = Blueprint('main', __name__)
import json
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from flask import Flask, request

@main.route("/12545/<int:year>/<int:month>/<int:day>/", methods=["GET"])

def segment_time_serie(year,month,day):
    ret  = Service.prediction.start(year, month, day)
    return json.dumps(ret)


def create_app():
    # declaro segment builder global

    app = Flask(__name__)
    app.register_blueprint(main)


    return app