from flask import Flask, Response, request
app = Flask(__name__)


@app.route("/raw_file/", methods=['GET'])
def hello():
    if 'date' in request.args:
        in_date = request.args['date']
    global date
    date = str(in_date)
    return '''
        <html><body>
        Hello. <a href="/getPlotCSV">Click me.</a>
        </body></html>
        '''


@app.route("/getPlotCSV")
def getPlotCSV():
    if date:
        return date
    else:
        csv = '1,2,3\n4,5,6\n'
        return Response(
            csv,
            mimetype="text/csv",
            headers={"Content-disposition":
                     "attachment; filename=myplot.csv"})


app.run(debug=True)
