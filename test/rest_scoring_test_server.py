"""Test server for logistic regression"""


from data_extract_transform_score.models import LogisticRegressionModel

from flask import Flask, request
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)


class TestLogisticRegressionModelRest(Resource):

    def post(self):
        input_dict = request.get_json()
        lrm = LogisticRegressionModel({"intercept": -5.0, "X": 2.0, "Y": 1.5})
        lrm_score = lrm.score(input_dict)

        return list(lrm_score)

api.add_resource(TestLogisticRegressionModelRest, '/logistic_regression/')

if __name__ == '__main__':
    app.run(debug=True)