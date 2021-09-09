import httpx
import base64
from esoper import Common
import json


class esOper:
    def __init__(self, EsHost: str, EsPort: int, Uname: str, Pwd: str, IndexName: str):
        """
        构造方法
        :param EsHost: ES访问的host，必须带上http://
        :param EsPort: ES访问的端口号
        :param Uname: ES授权用户名，如果没有设置就传''
        :param Pwd: ES授权密码，如果没有就传''
        :param IndexName: 要操作的索引名
        """
        # 将Uname和Pwd编写为head中用的授权字符串
        if Uname != '':
            self._authDict = {
                'Authorization': 'Basic ' + str(base64.b64encode(bytes(Uname + ':' + Pwd, encoding='utf-8')),
                                                encoding='utf-8')}
        else:
            self._authDict = {}
        # 将Host和Port拼接为baseUrl
        self._baseUrl = EsHost + ":" + str(EsPort)
        self._index = IndexName
        self._client = httpx.Client(http2=False, timeout=600)

    # ------------------------------------索引操作--------------------------------------------------------------
    def buildIndex(self, returnResultContent: bool, number_of_shards: int, number_of_replicas: int):
        """
        在ES中新建一个索引（索引名为init中的IndexName）
        :param returnResultContent: bool,是否返回结果内容
        :param number_of_shards: 索引分分片数量，必须在此指定好，后面无法修改
        :param number_of_replicas: 索引分片副本数量，后面可以修改
        :return: 根据参数返回不同形式，如果参数为true则返回response的具体内容，否则返回一个bool值代表是否成功新建索引
        """
        settingDict = {"settings": {
            "index": {
                "number_of_shards": number_of_shards,
                "number_of_replicas": number_of_replicas
            }
        }}
        if len(self._authDict) == 0:
            result = self._client.put(url=self._baseUrl + '/' + self._index, json=settingDict)
        else:
            result = self._client.put(url=self._baseUrl + '/' + self._index, json=settingDict, headers=self._authDict)
        if returnResultContent:
            return result.content
        else:
            successDict = {"acknowledged": True, "shards_acknowledged": True, "index": self._index}
            return Common.common.isSuccess(result.content, successDict)

    def addMapping(self, mappingDict: dict, returnResultContent: bool):
        '''
        添加自定义Mapping
        :param mappingDict: mapping dict
        :param returnResultContent: bool,是否返回结果内容
        :return: 根据参数返回不同形式，如果参数为true则返回response的具体内容，否则返回一个bool值代表是否成功添加mapping
        '''

        if len(self._authDict) == 0:
            result = self._client.put(url=self._baseUrl + '/' + self._index + '/_mapping', json=mappingDict)
        else:
            result = self._client.put(url=self._baseUrl + '/' + self._index + '/_mapping', json=mappingDict,
                                      headers=self._authDict)
        if returnResultContent:
            return result.content
        else:
            successDict = {"acknowledged": True}
            return Common.common.isSuccess(result.content, successDict)

    def setIndex(self, settingDict: dict, returnResultContent: bool):
        '''
        动态设置索引
        :param settingDict: 索引设置dict，动态设置包括：index.number_of_replicas //每个主分片的副本数。默认为
        1；index.auto_expand_replicas //基于可用节点的数量自动分配副本数量,默认为 false（即禁用此功能）；index.refresh_interval
        //执行刷新操作的频率，这使得索引的最近更改可以被搜索。默认为 1s，可以设置为 -1以禁用刷新；index.max_result_window //用于索引搜索的 from+size 的最大值。默认为
        10000；index.max_rescore_window // 在搜索此索引中 rescore 的 window_size 的最大值；index.blocks.read_only //设置为 true
        使索引和索引元数据为只读，false 为允许写入和元数据更改。index.blocks.read // 设置为 true 可禁用对索引的读取操作；index.blocks.write //设置为 true
        可禁用对索引的写入操作；index.blocks.metadata // 设置为 true 可禁用索引元数据的读取和写入；index.max_refresh_listeners
        //索引的每个分片上可用的最大刷新侦听器数 :
        :param returnResultContent: bool,是否返回结果内容
        :return: 根据参数返回不同形式，如果参数为true则返回response的具体内容，否则返回一个bool值代表是否设置成功
        '''
        if len(self._authDict) == 0:
            result = self._client.put(url=self._baseUrl + '/' + self._index + '/_settings', json=settingDict)
        else:
            result = self._client.put(url=self._baseUrl + '/' + self._index + '/_settings', json=settingDict,
                                      headers=self._authDict)
        if returnResultContent:
            return result.content
        else:
            successDict = {"acknowledged": True}
            return Common.common.isSuccess(result.content, successDict)

    def forceMerge(self, max_num_segments: int):
        '''
        强制合并段，段合并开销集中，最好在业务空闲时段做
        :param max_num_segments: 最大段数量，一般设为cpu核数/平均每个节点主分片数量
        :return: 返回response的具体内容
        '''
        paramDict = {"max_num_segments": max_num_segments}
        if len(self._authDict) == 0:
            result = self._client.post(url=self._baseUrl + '/' + self._index + '/_forcemerge', params=paramDict)
        else:
            result = self._client.post(url=self._baseUrl + '/' + self._index + '/_forcemerge', params=paramDict,
                                       headers=self._authDict)
        print(result.content)

    # ----------------------------------------增-----------------------------------------------------------------
    def insertLine(self, dataDict: dict, dataId: str, returnResultContent: bool):
        """
        向索引中插入一条数据
        :param dataDict: 数据字典
        :param dataIndex: 数据的id，如果让ES分配则传入None,否则传入一个唯一的str
        :param returnResultContent: bool,是否返回结果内容
        :return: 根据参数返回不同形式，如果参数为true则返回response的具体内容，否则返回一个bool值代表是否成功插入
        """
        if dataId is None:
            urlStr = self._baseUrl + '/' + self._index + '/_doc'
        else:
            urlStr = self._baseUrl + '/' + self._index + '/_doc/' + dataId
        if len(self._authDict) == 0:
            result = self._client.post(url=urlStr, json=dataDict)
        else:
            result = self._client.post(url=urlStr, json=dataDict, headers=self._authDict)
        if returnResultContent:
            return result.content
        else:
            successDict = {"result": "created"}
            return Common.common.isSuccess(result.content, successDict)

    # ----------------------------------------查-----------------------------------------------------------------
    def GetDataByQuery(self, sourceList: list, queryDict: dict, sortList: list, highlightDict: dict, fromInt: int,
                       sizeInt: int, timeout: str):
        """
        从索引中获取数据
        :param sourceList: _source中的list，不需要传None
        :param queryDict: 查询dict
        :param sortList: 排序dict的list，不需要传None
        :param highlightDict: 高亮dict，不需要传None
        :param fromInt: 分页查询起始值，不需要传None
        :param sizeInt: 分页查询页面大小，不需要传None
        :param timeout: 过期时间，不需要传None
        :return: 返回查询结果dict
        """
        searchDict = {'query': queryDict}
        if not sourceList is None:
            searchDict['_source'] = sourceList
        if not sortList is None:
            searchDict['sort'] = sortList
        if not highlightDict is None:
            searchDict['highlight'] = highlightDict
        if not fromInt is None:
            searchDict['from'] = fromInt
        if not sizeInt is None:
            searchDict['size'] = sizeInt
        if timeout is None:
            urlStr = self._baseUrl + '/' + self._index + '/_search'
        else:
            urlStr = self._baseUrl + '/' + self._index + '/_search?timeout=' + timeout
        if len(self._authDict) == 0:
            result = self._client.post(url=urlStr, json=searchDict)
        else:
            result = self._client.post(url=urlStr, json=searchDict, headers=self._authDict)
        return result.content

    def GetDataById(self, Id: str):
        """
        按照id从ES中取相应数据
        :param Id: 数据id
        :return: 取得的数据
        """
        urlStr = self._baseUrl + '/' + self._index + '/_doc/' + Id
        if len(self._authDict) == 0:
            result = self._client.get(url=urlStr)
        else:
            result = self._client.get(url=urlStr, headers=self._authDict)
        return result.content

    # ----------------------------------------删-----------------------------------------------------------------
    def DeleteByQuery(self, queryDict: dict, returnResultContent: bool):
        """
        按照查询条件删除数据
        :param queryDict: 查询跳间dict
        :param returnResultContent: bool,是否返回结果内容
        :return: 根据参数返回不同形式，如果参数为true则返回response的具体内容，否则返回删除的行数，如果出错返回False
        """
        searchDict = {'query': queryDict}
        urlStr = self._baseUrl + '/' + self._index + '/_delete_by_query'
        if len(self._authDict) == 0:
            result = self._client.post(url=urlStr, json=searchDict)
        else:
            result = self._client.post(url=urlStr, json=searchDict, headers=self._authDict)
        if returnResultContent:
            return result.content
        else:
            resultDict = json.loads(result.content)
            if 'deleted' in resultDict.keys():
                return resultDict['deleted']
            else:
                return False

    def DeleteById(self, Id: str, returnResultContent: bool):
        """
        按照ID删除数据
        :param Id: 要删除的数据id
        :param returnResultContent: bool,是否返回结果内容
        :return: 根据参数返回不同形式，如果参数为true则返回response的具体内容，否则返回一个bool值表示是否删除成功
        """
        urlStr = self._baseUrl + '/' + self._index + '/_doc/' + Id
        if len(self._authDict) == 0:
            result = self._client.delete(url=urlStr)
        else:
            result = self._client.delete(url=urlStr, headers=self._authDict)
        if returnResultContent:
            return result.content
        else:
            successDict = {"result": "deleted"}
            return Common.common.isSuccess(result.content, successDict)

    # ----------------------------------------改-----------------------------------------------------------------
    def UpdateById(self, Id: str, docDict: dict, upsertDict: dict, detect_noop: bool, returnResultContent: bool):
        """
        根据id更新数据
        :param Id:
        :param docDict:更新数据的dict
        :param upsertDict: upsert dict ,不需要则传None
        :param detect_noop: bool,如果值相同是否不修改
        :param returnResultContent: bool,是否返回结果内容
        :return:根据参数返回不同形式，如果参数为true则返回response的具体内容，否则返回一个bool值表示是否更新成功
        """
        urlStr = self._baseUrl + '/' + self._index + '/_update/' + Id
        dataDict = {'doc': docDict, "detect_noop": detect_noop}
        if not upsertDict is None:
            dataDict["upsert"] = upsertDict
        if len(self._authDict) == 0:
            result = self._client.post(url=urlStr, json=dataDict)
        else:
            result = self._client.post(url=urlStr, json=dataDict, headers=self._authDict)
        if returnResultContent:
            return result.content
        else:
            successDict = {"result": "updated"}
            return Common.common.isSuccess(result.content, successDict)

    def UpdateById_Script(self, Id: str, scriptDict: dict, upsertDict: dict, detect_noop: bool,
                          returnResultContent: bool):
        """
        根据id更新数据
        :param Id:
        :param scriptDict: painless脚本dict
        :param upsertDict: upsert dict ,不需要则传None
        :param detect_noop: bool,如果值相同是否不修改
        :param returnResultContent: bool,是否返回结果内容
        :return:根据参数返回不同形式，如果参数为true则返回response的具体内容，否则返回一个bool值表示是否更新成功
        """
        urlStr = self._baseUrl + '/' + self._index + '/_update/' + Id
        dataDict = {'script': scriptDict, "detect_noop": detect_noop}
        if not upsertDict is None:
            dataDict["upsert"] = upsertDict
        if len(self._authDict) == 0:
            result = self._client.post(url=urlStr, json=dataDict)
        else:
            result = self._client.post(url=urlStr, json=dataDict, headers=self._authDict)
        if returnResultContent:
            return result.content
        else:
            successDict = {"result": "updated"}
            return Common.common.isSuccess(result.content, successDict)

    def UpdateByQuery(self, scriptDict: dict, queryDict: dict, returnResultContent: bool):
        """
        根据查询条件更新数据
        :param scriptDict: painless脚本dict
        :param queryDict: 查询dict
        :param returnResultContent: bool,是否返回结果内容
        :return: 根据参数返回不同形式，如果参数为true则返回response的具体内容，否则返回更新的行数，如果出错返回False
        """
        urlStr = self._baseUrl + '/' + self._index + '/_update_by_query/'
        dataDict = {'script': scriptDict, 'query': queryDict}
        if len(self._authDict) == 0:
            result = self._client.post(url=urlStr, json=dataDict)
        else:
            result = self._client.post(url=urlStr, json=dataDict, headers=self._authDict)
        if returnResultContent:
            return result.content
        else:
            resultDict = json.loads(result.content)
            if 'updated' in resultDict.keys():
                return resultDict['updated']
            else:
                return False
