from flask import Flask

app = Flask(__name__)

@app.route("/")                      # adres URL: http://localhost:5000/
def home():
    return "Witaj w systemie monitoringu transakcji!"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
