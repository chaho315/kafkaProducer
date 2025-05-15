package org.example.kafkaproducer.service;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.kafkaproducer.controller.ProducerController;
import org.example.kafkaproducer.vo.EffectOrNot;
import org.example.kafkaproducer.vo.PurchaseLog;
import org.example.kafkaproducer.vo.PurchaseLogOneProduct;
import org.example.kafkaproducer.vo.WatchingAdLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class AdEvaluationService {
    //광고 데이터 중복 Join이 필요없다. --> Table
    //광고 이력이 먼저 들어옵니다.
    //구매 이력은 상품별로 들어오지 않습니다. (복수개의 상품 존재) --> contain
    //광고에 머문시간이 10초 이상되어야만 join 대상
    //특정 가격이상의 상품은 join 대상에서 제외(100만원)
    //광고이력 : KTable(AdLog), 구매이력 : KTable(PurchaseLogOneProduct)
    //filtering, 형 변환,
    //EffectOrNot --> Json 형태로 Topic : AdEvaluationComplete

    @Autowired
    Producer myprdc;

    @Autowired
    public void buildPipeline(StreamsBuilder sb){
        JsonSerializer<EffectOrNot> effectSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLog> purchaseLogSerializer = new JsonSerializer<>();
        JsonSerializer<WatchingAdLog> watchingAdLogSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLogOneProduct> purchaseLogOneProductSerializer = new JsonSerializer<>();

        JsonDeserializer<EffectOrNot> effectDeserializer = new JsonDeserializer<EffectOrNot>();
        JsonDeserializer<PurchaseLog> purchaseLogDeserializer = new JsonDeserializer<PurchaseLog>();
        JsonDeserializer<WatchingAdLog> watchingAdLogDeserializer = new JsonDeserializer<WatchingAdLog>();
        JsonDeserializer<PurchaseLogOneProduct> purchaseLogOneProductDeserializer = new JsonDeserializer<PurchaseLogOneProduct>();

        Serde<EffectOrNot> effectOrNotSerde = Serdes.serdeFrom(effectSerializer, effectDeserializer);
        Serde<PurchaseLog> purchaseLogSerde = Serdes.serdeFrom(purchaseLogSerializer, purchaseLogDeserializer);
        Serde<WatchingAdLog> watchingAdLogSerde = Serdes.serdeFrom(watchingAdLogSerializer, watchingAdLogDeserializer);
        Serde<PurchaseLogOneProduct> purchaseLogOneProductSerde = Serdes.serdeFrom(purchaseLogOneProductSerializer, purchaseLogOneProductDeserializer);

        //adLog steam --> table
        KTable<String, WatchingAdLog> adTable = sb.stream("AdLog", Consumed.with(Serdes.String(), watchingAdLogSerde))
                .selectKey((k, v) -> v.getUserId()+"_"+v.getProductId())
                .toTable(Materialized.<String, WatchingAdLog, KeyValueStore<Bytes, byte[]>>as("adStore")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(watchingAdLogSerde)
                );
        KStream<String, PurchaseLog> purchaseLogKStream = sb.stream("PurchaseLog", Consumed.with(Serdes.String(), purchaseLogSerde));
                //.filter((key,value) -> value.getPrice() < 1000000);

        purchaseLogKStream.foreach((k,v) -> {
            for(Map<String, String> prodInfo:v.getProductInfo()){

                if(Integer.valueOf(prodInfo.get("price")) < 1000000) {
                    PurchaseLogOneProduct tempVo = new PurchaseLogOneProduct();
                    tempVo.setUserId(v.getUserId());
                    tempVo.setProductId(prodInfo.get("productId"));
                    tempVo.setOrderId(v.getOrderId());
                    tempVo.setPrice(prodInfo.get("price"));
                    tempVo.setPurchasedDt(v.getPurchasedDt());

                    myprdc.sendJoinedMsg("oneProduct",tempVo);
                    sendNewMsg();
                }
            }
        });

        KTable<String, PurchaseLogOneProduct> purchaseLogOneProductKTable= sb.stream("PurchaseLogoneProduct", Consumed.with(Serdes.String(), purchaseLogOneProductSerde))
                .selectKey((k,v) -> v.getProductId()+"_"+v.getProductId())
                .toTable(Materialized.<String, PurchaseLogOneProduct, KeyValueStore<Bytes, byte[]>>as("purchaseLogStore")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(purchaseLogOneProductSerde));

        ValueJoiner<WatchingAdLog, PurchaseLogOneProduct, EffectOrNot> tableStreamJoiner = (leftValue, rightValue) -> {
            EffectOrNot returnValue = new EffectOrNot();
                returnValue.setUserId(rightValue.getUserId());
                returnValue.setAdId(leftValue.getAdId());
                returnValue.setOrderId(rightValue.getOrderId());
                Map<String, String> tempProdInfo = new HashMap<>();
                tempProdInfo.put("productId", rightValue.getProductId());
                tempProdInfo.put("price", rightValue.getPrice());
                returnValue.setProductInfo(tempProdInfo);
            return returnValue;
        };

        adTable.join(purchaseLogOneProductKTable, tableStreamJoiner).toStream().to("AdEvaluationComplete", Produced.with(Serdes.String(), effectOrNotSerde));
    }

    //대량의 테스트 데이터 생성 로직
    public void sendNewMsg() {
        PurchaseLog tempPurchaseLog  = new PurchaseLog();
        WatchingAdLog tempWatchingAdLog = new WatchingAdLog();

        //랜덤한 ID를 생성하기 위해 아래의 함수를 사용합니다.
        // random Numbers for concatenation with attrs
        Random rd = new Random();
        int rdUidNumber = rd.nextInt(9999);
        int rdOrderNumber = rd.nextInt(9999);
        int rdProdIdNumber = rd.nextInt(9999);
        int rdPriceIdNumber = rd.nextInt(90000)+10000;
        int prodCnt = rd.nextInt(9)+1;
        int watchingTime = rd.nextInt(55)+5;

        // bind value for purchaseLog
        tempPurchaseLog.setUserId("uid-" + String.format("%05d", rdUidNumber));
        tempPurchaseLog.setPurchasedDt("20230101070000");
        tempPurchaseLog.setOrderId("od-" + String.format("%05d", rdOrderNumber));
        ArrayList<Map<String, String>> tempProdInfo = new ArrayList<>();
        Map<String, String> tempProd = new HashMap<>();
        for (int i=0; i<prodCnt; i++ ){
            tempProd.put("productId", "pg-" + String.format("%05d", rdProdIdNumber));
            tempProd.put("price", String.format("%05d", rdPriceIdNumber));
            tempProdInfo.add(tempProd);
        }
        tempPurchaseLog.setProductInfo(tempProdInfo);

        // bind value for watchingAdLog
        tempWatchingAdLog.setUserId("uid-" + String.format("%05d", rdUidNumber));
        tempWatchingAdLog.setProductId("pg-" + String.format("%05d", rdProdIdNumber));
        tempWatchingAdLog.setAdId("ad-" + String.format("%05d",rdUidNumber) );
        tempWatchingAdLog.setAdType("banner");
        tempWatchingAdLog.setWatchingTime(String.valueOf(watchingTime));
        tempWatchingAdLog.setWatchingDt("20230201070000");

        // produce msg
        myprdc.sendMsgForPurchaseLog("purchaseLog", tempPurchaseLog);
        myprdc.sendMsgForWatchingAdLog("adLog", tempWatchingAdLog);
    }
}
