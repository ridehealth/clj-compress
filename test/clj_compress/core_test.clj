(ns clj-compress.core-test
  (:require
   [clj-compress.core :as compress]
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing]])
  (:import
   (java.io ByteArrayOutputStream)
   (org.apache.commons.io FileUtils)))

(deftest compress-data-test
  (testing "Compressing string"
    (let [s    "ABACABACABADEABACABACABADEABACABACABADEABACABACABADE"
          sbuf (.getBytes s)]
      (doseq [c compress/compressors]
        (let [cbuf        (ByteArrayOutputStream.)
              coutbuf     (ByteArrayOutputStream.)
              comp-size   (compress/compress-data sbuf cbuf c)
              decomp-size (compress/decompress-data (.toByteArray cbuf) coutbuf c)]
          (println (format "compressor: %13s, src size: %7s, compressed: %7s, decompressed: %7s."
                           c (.length s) (.size cbuf) decomp-size))
          (is (> comp-size 0))
          (is (= comp-size decomp-size))))))

  (testing "Compressing single file test"
    (let [in-file "data/test-file.txt"]
      (doseq [c compress/compressors]
        (let [out-file        (str in-file "." c)
              decomp-out-file (str in-file "." "txt")
              comp-size       (compress/compress-data in-file out-file c)
              decomp-size     (compress/decompress-data out-file decomp-out-file c)]
          (println (format "compressor: %13s, src size: %7s, compressed: %2s, decompressed: %2s."
                           c (.length (io/file in-file)) (.length (io/file out-file)) decomp-size))
          (io/delete-file out-file)
          (io/delete-file decomp-out-file)
          (is (> comp-size 0))
          (is (= comp-size decomp-size)))))))

(deftest create-archive-test
  (testing "compress / decompress folder test"
    (let [in-folder  "data/test-folder"
          out-folder "data/out/"]
      (doseq [c (filter #(not= "lz4-framed" %) compress/compressors)] ;; exclude lz4 cause it very very slow on big files
        (let [arch-name (compress/create-archive "test-arch" [in-folder] "data/" c)
              _         (compress/decompress-archive arch-name out-folder c)
              src-count (count (file-seq (io/file in-folder)))
              out-count (dec (count (file-seq (io/file out-folder))))]
          (println (format "compressor: %13s, arch-size: %7s, src folder count: %2s, decompressed folder count: %2s."
                           c (.length (io/file arch-name)) src-count out-count))
          (io/delete-file arch-name)
          (FileUtils/deleteDirectory (io/file out-folder))
          (is (= src-count out-count)))))))
