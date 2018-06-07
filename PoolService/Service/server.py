# Lanzarlo en local
# ~./spark-2.0.1-bin-hadoop2.7/bin/spark-submit server2.py
import time, sys, cherrypy, os
from paste.translogger import TransLogger
from app import create_app



def run_server(app):
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)

    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')

    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 8383,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()


if __name__ == "__main__":
    # Init spark context and load libraries

    app = create_app()

    # start web server
run_server(app)