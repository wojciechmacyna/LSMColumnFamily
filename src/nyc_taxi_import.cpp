#include "nyc_taxi_import.hpp"

#include "db_manager.hpp"

#include <string>
#include <vector>

void runNYCTaxiImport() {
    const std::string dbName = "data/NYCTaxiDB";
    const std::string csvFile = "data/yellow_tripdata_2016-03.csv";

    const std::vector<std::string> columns = {
        "pickup_location",
        "dropoff_location",
        "payment_type",
        "passenger_count",
        "vendor_id",
        "ratecode_id"
    };

    DBManager dbManager;

    // Open database and create required Column Families.
    dbManager.openDB(dbName, columns, true);

    // Import first 10 million CSV records.
    dbManager.importNYCTaxiCSV(
        csvFile,
        10'000'000,
        10'000);

    dbManager.closeDB();
}