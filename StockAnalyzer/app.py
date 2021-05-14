from flask import Flask, request
from flask_cors import CORS
from flask_restful import reqparse, abort, Api, Resource, fields, marshal_with
import platform

from server.admin.user import User
from server.job.job import Job
from server.code.stockcode import Code, CodeList, Price, OrderList

# from common.agent.ebest import EBest
# ebest = EBest("DEMO")
# ebest.login()
# result = ebest.get_code_list("ALL")
# print(result)
# print("result ok")
# ebest.logout()


# 윈도우 플렛폼 버전 확인
print(platform.architecture())

app = Flask(__name__)
CORS(app)
api = Api(app)

#https://flask-restful.readthedocs.io/en/0.3.3/intermediate-usage.html#full-parameter-parsing-example

api.add_resource(CodeList, "/codes", endpoint="codes")
api.add_resource(Code, "/codes/<string:code>", endpoint="code")
api.add_resource(Price, "/codes/<string:code>/price", endpoint="price")
api.add_resource(OrderList, "/orders", endpoint="orders")
api.add_resource(User, "/users", endpoint="users")
api.add_resource(User,  "/user/<string:id>", endpoint="id")
api.add_resource(Job,  "/job/<int:id>", endpoint="job")

if __name__ == "__main__":
    app.run(debug=True)