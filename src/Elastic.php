<?php
namespace app\index\tools;
use Elasticsearch\ClientBuilder;
use org\Elasticsearch;
use think\Exception;

/**
 * Created by PhpStorm.
 * User: zusux
 * Date: 2020/4/13
 * Time: 10:13
 * @author  zusux
 */
class elastic{
    protected $_table; //索引
    protected $_page; //分页
    protected $_limit = 20; //查询数量
    protected $_field = [];
    protected $_where = [];
    protected $_order = [];


    public function reset(){
        $this->_table = null;
        $this->_page = null;
        $this->_limit =20;
        $this->_field = [];
        $this->_where = [];
        $this->_order = [];
        return $this;
    }

    public function table(string $table){
        $this->reset();
        $this->_table  = $table;
        return $this;
    }

    /**
     * $condition='json'
     * $field = "attr"
     * $value = '[{"attr_name":"'.$v["attr_name"].'","attr_value":"'.$v["attr_value"].'"}]'
     *
     * $condition='range'
     * $field= 'use_date'
     * $value = ['+','+'] // start end
     *
     * $condition='in'
     * $field= 'id'
     * $value = [1,2,3]
     *
     * $condition='eq'
     * $field= 'id'
     * $value = 1
     *
     * $condition='like'
     * $field= 'name'
     * $value = '材料名称'
     */
    public function where(string $field,$value,string $condition='eq'){
        switch ($condition){
            case 'eq':
                $this->_where['filter'][] = ['term'=>[$field=>$value]];
                break;
            case 'like':
                $this->_where['must'][] = ['match'=>[$field=>['query'=>$value,'minimum_should_match'=>'50%']]];
                break;
            case 'cross_fields':
                //$map['name|spec'] = ['cross_fields','cross_fields',$name];
                $this->_where['must'][] = ['multi_match'=>['query'=>$value,'type'=>'cross_fields','fields'=>explode('|',$field)]];
                break;
            case 'in':
                $this->_where['filter'][] = ['terms'=>[$field=>$value]];
                break;
            case 'range':
                list($start,$end) = $value;
                if($start == '+' && $end != '+'){
                    $this->_where["must"][]['range'][$field] = ["lte"=>$end];
                }elseif($start != '+' && $end == '+'){
                    $this->_where["must"][]['range'][$field] = ["gte"=>$start];
                }elseif($start !='+' && $end !='+'){
                    $this->_where["must"][]['range'][$field] = ["gte"=>$start,"lte"=>$end];
                }
                break;
            case 'json':
                $json = json_decode($value,true);
                $tempValue = $json[0]['attr_value'];
                $tempValueArr = explode('**',$tempValue); // 查询多个属性值时 用**隔开
                $tempValueArr = array_filter($tempValueArr);
                if(count($tempValueArr)>1){
                    //多个 或查询
                    $should = [];
                    foreach($tempValueArr as $v){
                        $should[] = [
                            "match"=>[
                                "{$field}.attr_value"=>$v,
                            ]
                        ];
                    }
                    $insert = ["bool"=>["should"=>$should]];
                }else{
                    $insert = ["term"=>["{$field}.attr_value"=>$json[0]['attr_value']]];
                }
                $this->_where['must'][] = [
                    'nested'=>[
                        "path"=>$field,
                        "query"=>[
                            "bool"=>[
                                "must"=>[
                                    ["term"=>["{$field}.attr_name"=>$json[0]['attr_name']]],
                                    //["term"=>["{$value[1]}.attr_value"=>$json[0]['attr_value']]],
                                    //["bool"=>["should"=>[]]]
                                    $insert
                                ]
                            ]
                        ]
                    ],
                ];
                break;
            default:
                break;
        }

        return $this;
    }


    /*
     * $page 从0开始
     */
    public function page(int $page,int $limit=20){
        $this->_page = $page * $limit;
        $this->_limit = $limit;
        return $this;
    }

    public function order($field,$order='desc'){

        $this->_order[$field] = ['order'=>$order];
        return $this;
    }

    /**
     * @param $field
     */
    public function field($field){
        if(is_array($field)){
            $this->_field = $field;
        }else{
            $this->_field = explode(',',$field);
        }

        return $this;
    }



    public function select(){
        $client = ClientBuilder::create()->build();
        if(!$this->_table){
            throw new Exception('索引未知');
        }
        $params = [
            'index' => $this->_table,
            'body' => [
            ]
        ];
        if($this->_page !== null ){
            $params['body']['from'] = $this->_page;
            $params['body']['size'] = $this->_limit;
        }
        if(!empty($this->_order)){
            $params['body']['sort'] = $this->_order;
        }
        if(!empty($this->_order)){
            $params['body']['sort'] = $this->_order;
        }
        if(!empty($this->_where)){
            $params['body']['query'] = ['bool'=>$this->_where];
        }
        if(!empty($this->_field)){
            $params['body']['_source'] = $this->_field;
        }

        $this->reset();
        $result = $client->search($params);


        //print_r($result);exit;

        $took = $result['took'] ?? 0;
        $total = $result['hits']['total']['value'] ?? 0;
        $source = $result['hits']['hits'] ?? [];

        $max = $result['hits']['max_score'] ? :0;
        if($source && !$max){
            $max = $source[0]['_score'];
        }

        $data = [];
        foreach($source as $k=>$item){
            $data[$k] = $item['_source'] ?? [];
        }

        $return = [
            'total'=>$total,
            'took'=>$took,
            'list'=>$data,
            'max'=>$max,
            'from'=>'es'
        ];

        return $return;
    }

    /**
     * @param $id
     * @param array $field
     * @return int 0|1
     * @desc 创建索引并添加一条记录
     */
    public function index($id,$field=[]){
        $client = ClientBuilder::create()->build();
        $params = [
            'index' => $this->_table,
            'id' => $id,
            'body' => $field // 此条数据的内容，数组可以任意定义。
        ];
        $this->reset();
        $response = $client->index($params);

        return $response['_shards']['successful'] ?? 0;
    }

    public function upsert($id,$field=[]){
        $client = ClientBuilder::create()->build();
        $params = [
            'index' => $this->_table,
            'id' => $id,
            'body' => $field // 此条数据的内容，数组可以任意定义。
        ];
        $this->reset();
        $response = $client->update($params);

    }


    /**
     * 获取一条记录
     * @return false|array
     */
    public function get($id){
        $client = ClientBuilder::create()->build();
        $params = [
            'index' => $this->_table,
            'id' => $id
        ];
        try{
            $response = $client->get($params);
            $this->reset();
            if($response['found']){
                return $response['_source'];
            }
        }catch ( \Elasticsearch\Common\Exceptions\Missing404Exception $e){
            return false;
        }
    }
    /*
     * @desc 删除一条记录
     */
    public function delete($id){
        $client = ClientBuilder::create()->build();
        $params = [
            'index' => $this->_table,
            'id' => $id
        ];
        $this->reset();
        $response = $client->delete($params);
        return $response['_shards']['successful'] ?? 0;
    }

    /*
     * @desc 删除查询条记录
     * @return number|false
     */
    public function deleteByQuery(){
        $client = ClientBuilder::create()->build();
        $params = [
            'index' => $this->_table,
            'body' => [
            ]
        ];
        if(!empty($this->_where)){
            $params['body']['query'] = ['bool'=>$this->_where];
        }
        $this->reset();
        $response = $client->deleteByQuery($params);
        return $response["deleted"] ?? false;
    }



    /*
     * 删除索引
     */
    public function drop_index(){
        $client = ClientBuilder::create()->build();
        $deleteParams = [
            'index' => $this->_table
        ];
        $this->reset();
        $response = $client->indices()->delete($deleteParams);
        return $response['acknowledged'] ?? false;
    }
    /**
     * 创建索引
     * @return bool true|false
     */
    public function create_index($shards=2){
        $client = ClientBuilder::create()->build();
        $params = [
            'index' => $this->_table,
            'body' => [
                'settings' => [               // 自定义设置配置
                    'number_of_shards' => $shards,  // 数据分片数
                    'number_of_replicas' => 0 // 数据备份数
                ]
            ]
        ];
        $this->reset();
        $response = $client->indices()->create($params);
        return $response['acknowledged'] ?? false;
    }


    /**
     * @desc 更新数据
     * @desc $result 二维数组 一条记录
     * @return int 0|1
     */
    public function update($id,array $result){
        $client = ClientBuilder::create()->build();
        $params = [
            'index' => $this->_table,
            'id' => $id,
            'body' => [
                'doc' => $result
            ]
        ];
        try{
            $this->reset();
            $response = $client->update($params);
            return $response['_shards']['successful'] ?? 0;
        }catch (\Elasticsearch\Common\Exceptions\Missing404Exception $e){
            return 0;
        }

    }

}
