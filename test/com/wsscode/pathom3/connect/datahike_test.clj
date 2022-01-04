(ns com.wsscode.pathom3.connect.datahike-test
  (:require
   [clojure.test :refer [deftest is are run-tests testing]]
   [com.wsscode.pathom3.connect.built-in.plugins :as pbip]
   [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
   [com.wsscode.pathom3.connect.datomic :as pcd]
   [com.wsscode.pathom3.connect.datahike :refer [datahike-config]]
   [com.wsscode.pathom3.connect.indexes :as pci]
   [com.wsscode.pathom3.connect.operation :as pco]
   [com.wsscode.pathom3.interface.eql :as p.eql]
   [com.wsscode.pathom3.plugin :as p.plugin]
   [datahike.api :as d]
   [edn-query-language.core :as eql]))

(def db-schema
  [#:db{:ident :person/name
        :cardinality :db.cardinality/one
        :unique :db.unique/identity
        :valueType :db.type/string}
   #:db{:ident :person/friends
        :cardinality :db.cardinality/many
        :valueType :db.type/ref}])

(def dhcfg {:store {:backend :mem :id "default"}})
(d/create-database dhcfg)
(def conn (d/connect dhcfg))
(d/transact conn db-schema)
(d/transact conn
            [{:db/id -1
              :person/name "alpha"
              :person/friends [{:db/id -2
                                :person/name "bravo"}
                               {:db/id -3
                                :person/name "charlie"
                                :person/friends [-2]}]}])

(def db (d/db conn))

(def db-config
  (assoc datahike-config
         ::pco/op-name `mbrainz))

(def db-schema-output
  {:person/name #:db{:ident :person/name
                     :cardinality :db.cardinality/one
                     :unique :db.unique/identity
                     :valueType {:db/ident :db.type/string}}
   :person/friends #:db{:ident :person/friends
                        :cardinality :db.cardinality/many
                        :valueType {:db/ident :db.type/ref}}})

(deftest test-db->schema
  (is (= (pcd/db->schema db-config db)
         db-schema-output)))

(pco/defresolver long-name [{:person/keys [name]}]
  {:person/long-name (str name "a;skdfj")})

(def registry
  [long-name])

(def connect-cfg (assoc db-config
                        ::pcd/conn conn
         ;; ::pcd/admin-mode? true
                        ::pcd/ident-attributes #{}))

(def env
  (-> (pci/register
       [registry])
      (pcd/connect-datomic connect-cfg)))

(def request (p.eql/boundary-interface env))

(deftest test-datomic-parser

  (testing "process-query"
    (is (= (request [])
           {:artist/artists-before-1600
            [{:artist/super-name "SUPER - Heinrich Schütz",
              :artist/country    {:country/name "Germany"}}
             {:artist/super-name "SUPER - Choir of King's College, Cambridge",
              :artist/country    {:country/name "United Kingdom"}}]}))

    (is (= (request {}
                    [:person/long-name])
           {:artist/artist-before-1600
            {:artist/super-name "SUPER - Heinrich Schütz",
             :artist/country    {:country/name "Germany"}}}))

    (testing "partial missing information on entities"
      (is (= (request {::pcd/db (-> (d/with db
                                            [{:medium/name "val"}
                                             {:medium/name "6val"
                                              :artist/name "bla"}
                                             {:medium/name "3"
                                              :artist/name "bar"}])
                                    :db-after)}
                      [{:all-mediums
                        [:artist/name :medium/name]}])
             {:all-mediums [{:artist/name "bar", :medium/name "3"}
                            {:medium/name                                         "val",
                             :com.wsscode.pathom3.connect.runner/attribute-errors {:artist/name {:com.wsscode.pathom3.error/error-type :com.wsscode.pathom3.error/attribute-unreachable}}}
                            {:artist/name "bla", :medium/name "6val"}]})))

    (testing "without subquery"
      (is (= (request {}
                      [:artist/artists-before-1600])
             {:artist/artists-before-1600
              [{} {}]}))

      (is (= (request {}
                      [:artist/artist-before-1600])
             {:artist/artist-before-1600
              {}})))))

(def env-admin
  (-> (pci/register
       [registry])
      (pcd/connect-datomic
       (assoc db-config
              ::pcd/conn conn
              ::pcd/admin-mode? true
              ::pcd/ident-attributes #{:artist/type}))))

(def request-admin (p.eql/boundary-interface env-admin))

(deftest datomic-admin-mode-test
  (testing "reading from :db/id"
    (is (= (request-admin
            [{[:db/id 637716744120508]
              [:artist/name]}])
           {[:db/id 637716744120508] {:artist/name "Janis Joplin"}})))

  (testing "reading from :db/id"
    (is (= (request-admin
            [{[:db/id 637716744120508]
              [:artist/name]}])
           {[:db/id 637716744120508] {:artist/name "Janis Joplin"}})))

  (testing "reading from unique attribute"
    (is (= (request-admin [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
                            [:artist/name]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/name "Janis Joplin"}})))

  (testing "ident attributes"
    (is (= (request-admin
            [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
              [:artist/type]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/type :artist.type/person}})))

  (testing "explicit db"
    (is (= (request-admin {::pcd/db (:db-after (d/with (d/db conn)
                                                       [{:artist/gid  #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"
                                                         :artist/name "not Janis Joplin"}]))}
                          [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
                            [:artist/name]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/name "not Janis Joplin"}})))

  (testing "nested complex dependency"
    (is (= (request-admin {}
                          [{[:release/gid #uuid"b89a6f8b-5784-41d2-973d-dcd4d99b05c2"]
                            [{:release/artists
                              [:artist/super-name]}]}])
           {[:release/gid #uuid"b89a6f8b-5784-41d2-973d-dcd4d99b05c2"]
            {:release/artists [{:artist/super-name "SUPER - Horst Jankowski"}]}})))

  (testing "implicit dependency"
    (is (= (request-admin {}
                          [{[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
                            [:artist/super-name]}])
           {[:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"]
            {:artist/super-name "SUPER - Janis Joplin"}}))))
