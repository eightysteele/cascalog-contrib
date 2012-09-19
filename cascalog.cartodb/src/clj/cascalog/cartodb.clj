(ns cascalog.cartodb
  (:use cascalog.api)
  (:require [cascalog.workflow :as w])
  (:import [cascalog.cartodb CartoDBTap CartoDBScheme]))

(defn cartodb-tap
  [account api-key sql sql-count]
  (let [tap (CartoDBTap/create (CartoDBScheme.) account api-key sql sql-count)]
    (cascalog-tap tap (stdout))))

(comment
  (let [account "vertnet"
        api-key "b3263ddb2c8afddfa4bddca43d44727f9d3220ce"
        sql "select * from publishers"
        sql-count "select count(*) as count from publishers"        
        source (cartodb-tap account api-key sql sql-count)]
    (prn source)
    (?<- (stdout)         
         [?line]
         (source ?line))))
