def moop(n):
    import io
    import numpy as np
    import rasterio

    from flask import Flask
    from flask import make_response
    from PIL import Image

    app = Flask(__name__)
    app.reader = None

    @app.route('/ping')
    def ping():
        return 'pong\n'

    @app.route("/<rdd>/<int:zoom>/<int:x>/<int:y>.png")
    def tile(rdd, zoom, x, y):
        arr = np.zeros([256, 256]) + 127
        image = Image.fromarray(arr.astype('uint8')).convert('L')
        bio = io.BytesIO()
        image.save(bio, 'PNG')
        response = make_response(bio.getvalue())
        response.headers['Content-Type'] = 'image/png'
        return response

    app.run(host='0.0.0.0', port=8033)

if __name__ == "__main__":
    moop(42)
