package com.jx.elasticsearch;

import com.jx.elasticsearch.service.IndexFieldService;
import com.jx.elasticsearch.utils.http.HttpHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticsearchApplicationTests {

	@Autowired
	private IndexFieldService indexFieldService;

	@Test
	public void contextLoads() {
	}

	/*@Test
	public void timetest(){
		System.out.println(new Date().getTime());
		System.out.println(System.currentTimeMillis());
		String time = "2018-04-08 23:59:59";
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
		try {
			Date parse = format.parse(time);
			System.out.println(parse.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}*/

   /* @Test
	public void updateTest(){
        Map<String,JSONObject> map = new HashMap<>();
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("content","eeeee");
		map.put("1",jsonObject);
		JSONObject jsonObject1 = new JSONObject();
		jsonObject1.put("content","ffff");
		map.put("2",jsonObject1);
		JSONObject jsonObject2 = new JSONObject();
		jsonObject2.put("content","ggggg");
		map.put("4",jsonObject2);
		JSONObject jsonObject3 = new JSONObject();
		jsonObject3.put("content","HHHHHH");
		map.put("5",jsonObject3);
		ElasticsearchUtils.batchUpdateData("index","fulltext",map);
	}*/
	/*@Test
	public void inTest(){
		List<Long> list = new ArrayList<>();
		list.add(52510008575489l);
		list.add(52510027794689l);
		ElasticsearchUtils.searchDataByIn("serviceorder","sou",list);
	}*/
	/*@Test
	public void exactTest(){
		Map<String,String> map = new HashMap<>();
		map.put("user.userId","54757309404673");
		List list = ElasticsearchUtils.searchExactToFileds("serviceorder", "sou", map);
		System.out.println(list.size());
		System.out.println(list);
	}*/
	/*@Test
	public void searchDataByInTest(){
		List<String> list = new ArrayList<>();
		list.add("52510008575489");
		ElasticsearchUtils.searchDataByIn("serviceorder","sou",list);
	}*/
	/*@Test
   public void timetest(){
		try {
			long start = System.currentTimeMillis();
			String s = HttpHelper.get("http://192.168.7.252:8088/serviceorder/getServiceOrderIdsByUserId?userId=");
			long end = System.currentTimeMillis();
			System.out.println("查询全部服务单共耗时："+(end-start));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}*/
}
