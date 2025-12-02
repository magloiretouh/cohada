from flask import Flask
from routes import register_routes
from flask_cors import CORS
from logging_config import setup_logging
import logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configure CORS for production
# Restrict to specific origins instead of allowing all
CORS(
    app,
    supports_credentials=True,
    origins=["http://localhost:3000", "http://127.0.0.1:5503"],  # Add your frontend URLs
    allow_headers=["Content-Type", "Authorization"],
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"]
)

# Register routes
register_routes(app)

# Error handlers with logging
@app.errorhandler(404)
def not_found(error):
    logger.warning(f"404 Not Found: {error}")
    return {"error": "Resource not found"}, 404

@app.errorhandler(500)
def server_error(error):
    logger.error(f"500 Server Error: {error}", exc_info=True)
    return {"error": "Internal server error"}, 500

if __name__ == '__main__':
    logger.info("Starting OHADA Reporting API")
    app.run()