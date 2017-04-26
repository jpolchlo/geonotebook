def moop(n):
    from flask import Flask, make_response
    from flask_cors import cross_origin

    app = Flask(__name__)
    app.reader = None
    print('FLASK')

    @app.route('/shutdown')
    def shutdown():
        raise Exception

    @app.route("/ping")
    def ping():
        return 'pong'

    app.run(host='0.0.0.0', port=8033)
