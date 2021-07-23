from amazon_wf import app


if __name__ == '__main__':
    # Quick test configuration. Please use proper Flask configuration options
    # in production settings, and use a separate file or environment variables
    # to manage the secret key!
    app.run(host='0.0.0.0', port=4995, debug=True)
