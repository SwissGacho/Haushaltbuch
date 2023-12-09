# Import Flask from its treasure trove
from flask import Flask
from test import HaushaltDB


# Create our Flask ship to sail the web seas
app = Flask(__name__)

# Define the home port route, where our journey begins
@app.route('/')
def home():
    # The message to be displayed, like a flag hoisted high
    return "Ahoy! Welcome to our Flask server!"

# Define a route to insert a currency
@app.route('/insert_currency/<currency_name>')
def insert_currency(currency_name: str):
    db = HaushaltDB()
    db.insert_currency(currency_name)
    return f"Yarrr! The currency {currency_name} be inserted, a deed as glorious as findin' a sunken treasure!"


# Set sail! But beware, for here be dragons!
if __name__ == '__main__':
    app.run(debug=True)  # The debug flag, a lookout in the crow's nest