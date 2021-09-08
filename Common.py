import  json
class common:

    @staticmethod
    def isSuccess( result, successDict: dict):
        result = json.loads(result)
        flag = True
        for successItem in successDict.keys():
            if not (successItem in result.keys() and successDict[successItem] == result[successItem]):
                flag = False
                break
        return flag
