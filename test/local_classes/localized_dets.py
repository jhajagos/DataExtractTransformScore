from data_extract_transform_score import models
from math import exp


class LogLinearRegressionModel(models.GeneralizedLinearModel):
    def _compute_score_using_model(self, coefficients):
        return exp(sum(coefficients))


# Add new models here
LOCAL_MODELS_TO_REGISTER = [("Log-linear regression", LogLinearRegressionModel)]


def lower_case(data):

    for key in data:

        for element in data[key]:
            if "code" in element:
                element["code"] = element["code"].lower()

    return (data, None)

# Add new custom transformations
LOCAL_TRANSFORMATIONS_TO_REGISTER = [("lower_case", lower_case)]