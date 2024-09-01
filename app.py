
from flask import Flask
from libs.getFile import *


app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/getData')
def getData():
    result = getcsv()
    
    return result

if __name__ == '__main__':  
   app.run()  