from flask import Flask

app = Flask(__name__)

@app.route("/")                      # adres URL: http://localhost:5000/
def home():
    return "Witaj w systemie monitoringu transakcji!"

@app.route("/hello")                      
def abc():
    a = request.args.get("a", 0, float)
    b = request.args.get("b", 0, float)
    return f"suma {a} i {b} wynosi {a+b}"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
