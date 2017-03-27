import math
import requests
import json


class ModelsRegistry(object):
    """Registers a model name with a model class"""

    def __init__(self, model_name_class_tuples_list=None):

        hard_coded_model_name_class_tuples = [("Logistic regression", LogisticRegressionModel),
                                              ("Linear regression", LinearRegressionModel),
                                              ("HTTP REST Model", HTTPRestModel),
                                              ("Openscoring REST Model", OpenScoringRestModel)
                                              ]

        if model_name_class_tuples_list is None:
            model_name_class_tuples_list = hard_coded_model_name_class_tuples
        else:
            model_name_class_tuples_list += hard_coded_model_name_class_tuples

        self.model_name_class_dict = {}
        for model_name_class in model_name_class_tuples_list:
            model_name, model_class = model_name_class
            self.model_name_class_dict[model_name] = model_class


class PredictiveModel(object):
    """Base class for a predictive model"""
    def __init__(self, parameters):
        self.parameters = parameters

    def score(self, input_dict):
        return (0.0, None)


class GeneralizedLinearModel(PredictiveModel):
    def _pair_with_coefficients(self, pair1, pair2):

        paired_list = [(pair1[i], pair2[i]) for i in range(len(pair1))]
        paired_list.sort(key=lambda x: x[1], reverse=True)

        return paired_list

    def score(self, input_dict):

        input_dict["intercept"] = 1.0

        coefficients_included = []
        variables_included = []
        for key in input_dict:
            if key in self.parameters:
                variables_included += [key]
                coefficients_included += [self.parameters[key] * input_dict[key]]

        coefficients_included_paired = self._pair_with_coefficients(variables_included, coefficients_included)

        variables_not_included = []
        coefficients_not_included = []
        for key in self.parameters:
            if key not in variables_included:
                variables_not_included += [key]
                coefficients_not_included += [self.parameters[key]]

        coefficients_not_included_pairs = self._pair_with_coefficients(variables_not_included, coefficients_not_included)

        return (self._compute_score_using_model(coefficients_included),
                {"coefficients_included": coefficients_included_paired,
                 "coefficients_not_included": coefficients_not_included_pairs})

    def _compute_score_using_model(self, input_dict):
        return None


class LogisticRegressionModel(GeneralizedLinearModel):
    def _compute_score_using_model(self, coefficients):
        return math.exp(sum(coefficients)) / (1 + math.exp(sum(coefficients)))


class LinearRegressionModel(GeneralizedLinearModel):
    def _compute_score_using_model(self, coefficients):
        return sum(coefficients)


class MultipleKeyedModels(PredictiveModel):
    """Model based on key"""

    def set_keyed_model(self, keyed_models_dict, key_map_func=None):

        self.keyed_models_dict = keyed_models_dict
        self.key_map_func = key_map_func

    def score(self, input_dict):

        model_obj = self.map_func(input_dict)
        return model_obj.score(input_dict)


class BuildMultipleKeyedModel(object):
    """Generate a keyed model object"""
    def __init__(self, keyed_models_dict, key_map_func=None, keys_to_map=None):

        if keys_to_map is not None:
            key_map_func = lambda input_dict: input_dict[keys_to_map]

        self.keyed_models_dict = keyed_models_dict
        self.key_map_func = key_map_func

    def generate(self, parameters={}):
        multi_key_obj = MultipleKeyedModels(parameters)
        multi_key_obj.set_keyed_model(self.keyed_models_dict, self.key_map_func)
        return multi_key_obj


class HTTPRestModel(PredictiveModel):
    """A base model that calls an HTTP response"""

    def _post_json_with_json_response(self, url, object_to_json):
        r_obj = requests.post(url, json=object_to_json)
        json_obj = r_obj.json()
        return json_obj

    def _get_with_json_response(self, url):
        r_obj = requests.get(url)
        json_obj = r_obj.json()
        return json_obj

    def score(self, input_dict):
        score_url = self.parameters["url"]
        if "method" in self.parameters:
            method = self.parameters["method"]
        else:
            method = "POST"

        if method == "POST":
            return self._post_json_with_json_response(score_url, input_dict)


class OpenScoringRestModel(HTTPRestModel):
    def score(self, input_dict):

        # TODO: Still debugging this against a sample file

        # input_dict[u"Y"] = 0

        openscoring_model_url = self.parameters["url"]
        model_details = self._get_with_json_response(openscoring_model_url)

        request_struct = {"id": "test1", "arguments": input_dict}
        # print(request_struct)
        model_response = self._post_json_with_json_response(openscoring_model_url, request_struct)
        # print(model_response)

        return (model_response, model_details)


