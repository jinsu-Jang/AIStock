import os
import sys
sys.path.append( os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource, fields, marshal_with

import json
# from bson import json_util
from bson.objectid import ObjectId


from common.db_handler.mongodb_handler import MongoDBHandler
mongodb = MongoDBHandler()

class User(Resource):
    def get(self, id=None, users=None):
        if id:
            print("specific user")
            return "specific user"
        else:
            status = request.args.get('status', default="all", type=str)
            if status == 'all':
                result_list = list(mongodb.find_items({"isdeleted": "0"}, "stock", "user"))
            elif status in ["buy_ordered", "buy_completed", "sell_ordered", "sell_completed"]:
                result_list = list(mongodb.find_items({"status":status}, "stock", "user"))
            else:
                return {}, 404
            print(result_list)

            result_json = []
            if result_list : 
                for result in result_list :
                    result['id'] = str(result['_id'])
                    del result['_id']
                    result_json.append(result)

            print(result_json)
            # print({ "count": len(result_list), "user_list": json.dumps(result_list, default=json_util.default) }, 200)
            return { "count": len(result_list), "user_list": result_json }, 200    

    def post(self):
        try:
            parser = reqparse.RequestParser()
            # parser.add_argument('image', type=str)
            parser.add_argument('name', type=str)
            parser.add_argument('birthday', type=str)
            parser.add_argument('gender', type=str)
            parser.add_argument('job', type=str)
            args = parser.parse_args()

            # _image = args['image']
            _name = args['name']
            _birthday = args['birthday']
            _gender = args['gender']
            _job = args['job']

            result =  mongodb.insert_item({'name': args['name'], 'birthday': args['birthday'], 'gender': args['gender'], 'job': args['job'], 'isdeleted': "0"}, "stock", "user")
            
            result_list = list(mongodb.find_items({"isdeleted": "0"}, "stock", "user"))
            # return { "count": len(result_list), "user_list": result_list }, 200 
            return { "count": len(result_list), "user_list": json.dumps(result_list, default=json_util.default) }, 200 
            # return {'name': args['name'], 'birthday': args['birthday']}
            
        except Exception as e:
            return {'error': str(e)}

    def delete(self, id=None):
        if id:
            cond = {'_id': ObjectId(id)}
            # result = mongodb.delete_items(cond, "stock", "user")
            result = mongodb.update_item(cond, { "$set": {"isdeleted": "1"} }, "stock", "user")
            print("deleting user")
            return "deleting user"
        else:
            print("need to specify a user")
            return "need to specify a user"


class UserList(Resource):
    def get(self):
        status = request.args.get('status', default="all", type=str)
        if status == 'all':
            result_list = list(mongodb.find_items({"isdeleted": "0"}, "stock", "user"))
        elif status in ["buy_ordered", "buy_completed", "sell_ordered", "sell_completed"]:
            result_list = list(mongodb.find_items({"status":status}, "stock", "user"))
        else:
            return {}, 404
        print(result_list)

        result_json = []
        if result_list : 
            for result in result_list :
                result['id'] = str(result['_id'])
                del result['_id']
                result_json.append(result)

        print(result_json)
        # print({ "count": len(result_list), "user_list": json.dumps(result_list, default=json_util.default) }, 200)
        return { "count": len(result_list), "user_list": result_json }, 200     
        # return { "count": len(result_list), "user_list": result_list }, 200     
        # json.dumps(docs_list, default=json_util.default)

    def post(self):
        try:
            parser = reqparse.RequestParser()
            # parser.add_argument('image', type=str)
            parser.add_argument('name', type=str)
            parser.add_argument('birthday', type=str)
            parser.add_argument('gender', type=str)
            parser.add_argument('job', type=str)
            args = parser.parse_args()

            # _image = args['image']
            _name = args['name']
            _birthday = args['birthday']
            _gender = args['gender']
            _job = args['job']

            result =  mongodb.insert_item({'name': args['name'], 'birthday': args['birthday'], 'gender': args['gender'], 'job': args['job'], 'isdeleted': "0"}, "stock", "user")
            
            result_list = list(mongodb.find_items({"isdeleted": "0"}, "stock", "user"))
            # return { "count": len(result_list), "user_list": result_list }, 200 
            return { "count": len(result_list), "user_list": json.dumps(result_list, default=json_util.default) }, 200 
            # return {'name': args['name'], 'birthday': args['birthday']}
            
        except Exception as e:
            return {'error': str(e)}

    def delete(self):
        try:
            print("delete 실행")
            result = mongodb.update_item({"단축코드":code}, "stock", "code_info")
             
        except Exception as e:
            return {'error': str(e)}            