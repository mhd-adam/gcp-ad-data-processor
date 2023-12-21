-- noinspection SqlNoDataSourceInspectionForFile

-- Trigger to remove duplicates after the insertion of new data
-- Run after to check new data against the old data
CREATE OR REPLACE FUNCTION ad_data_remove_dups_count()
RETURNS TRIGGER AS $$
DECLARE
    duplicate_count INT;
BEGIN
    -- Get the count of duplicates
    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT "Index"
        FROM (
            SELECT "Index",
                   ROW_NUMBER() OVER


                       (

                       PARTITION BY
                       "Time",
                                  "AdvertiserId",
                                  "OrderId",
                                  "LineItemId",
                                  "CreativeId",
                                  "CreativeVersion",
                                  "CreativeSize",
                                  "AdUnitId",
                                  "Domain",
                                  "CountryId",
                                  "RegionId",
                                  "MetroId",
                                  "CityId",
                                  "BrowserId",
                                  "OSId",
                                  "OSVersion",
                                  "TimeUsec2",
                                  "KeyPart",
                                  "Product",
                                  "RequestedAdUnitSizes",
                                  "BandwidthGroupId",
                                  "MobileDevice",
                                  "IsCompanion",
                                  "DeviceCategory",
                                  "ActiveViewEligibleImpression",
                                  "MobileCarrier",
                                  "EstimatedBackfillRevenue",
                                  "GfpContentId",
                                  "PostalCodeId",
                                  "BandwidthId",
                                  "AudienceSegmentIds",
                                  "MobileCapability",
                                  "PublisherProvidedID",
                                  "VideoPosition",
                                  "PodPosition",
                                  "VideoFallbackPosition",
                                  "IsInterstitial",
                                  "EventTimeUsec2",
                                  "EventKeyPart",
                                  "YieldGroupCompanyId",
                                  "RequestLanguage",
                                  "DealId",
                                  "SellerReservePrice",
                                  "DealType",
                                  "AdxAccountId",
                                  "Buyer",
                                  "Advertiser",
                                  "Anonymous",
                                  "ImpressionId"
                   ) AS rnum
            FROM "Ad_Data_Adam"
        ) as t
        WHERE t.rnum > 1
    ) as tI;

    INSERT INTO "Ad_Data_Adam_duplicate_count" (duplicate_count, ts)
    VALUES (duplicate_count, NOW());

    DELETE FROM "Ad_Data_Adam"
    WHERE ("Index") IN (
        SELECT "Index"
        FROM (
            SELECT "Index",
                   ROW_NUMBER() OVER (PARTITION BY
                      "Time",
                                  "AdvertiserId",
                                  "OrderId",
                                  "LineItemId",
                                  "CreativeId",
                                  "CreativeVersion",
                                  "CreativeSize",
                                  "AdUnitId",
                                  "Domain",
                                  "CountryId",
                                  "RegionId",
                                  "MetroId",
                                  "CityId",
                                  "BrowserId",
                                  "OSId",
                                  "OSVersion",
                                  "TimeUsec2",
                                  "KeyPart",
                                  "Product",
                                  "RequestedAdUnitSizes",
                                  "BandwidthGroupId",
                                  "MobileDevice",
                                  "IsCompanion",
                                  "DeviceCategory",
                                  "ActiveViewEligibleImpression",
                                  "MobileCarrier",
                                  "EstimatedBackfillRevenue",
                                  "GfpContentId",
                                  "PostalCodeId",
                                  "BandwidthId",
                                  "AudienceSegmentIds",
                                  "MobileCapability",
                                  "PublisherProvidedID",
                                  "VideoPosition",
                                  "PodPosition",
                                  "VideoFallbackPosition",
                                  "IsInterstitial",
                                  "EventTimeUsec2",
                                  "EventKeyPart",
                                  "YieldGroupCompanyId",
                                  "RequestLanguage",
                                  "DealId",
                                  "SellerReservePrice",
                                  "DealType",
                                  "AdxAccountId",
                                  "Buyer",
                                  "Advertiser",
                                  "Anonymous",
                                  "ImpressionId"
                   ) AS rnum
            FROM "Ad_Data_Adam"
        ) t
        WHERE t.rnum > 1
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER ad_data_remove_dups_count_triffer
after INSERT ON "Ad_Data_Adam"
EXECUTE FUNCTION ad_data_remove_dups_count();








-- 1. Records by date and hour
select "Time"::Date as day , extract (hour from "Time") as hour, count(*) as records
from "Ad_Data_Adam"
group by day, hour
order by day, hour;


-- 2. the total of the EstimatedBackFillRevenue field per day and per hour
select "Time"::Date as day , extract (hour from "Time") as hour, sum("EstimatedBackfillRevenue") as tot_reve
from "Ad_Data_Adam"
group by day, hour
order by day, hour;

-- 3. records and the total of the EstimatedBackFillRevenue per Buyer
select "Buyer", count(*) records, sum("EstimatedBackfillRevenue") as tot_reve
from "Ad_Data_Adam"
group by "Buyer";

-- 4.a. list of advertisers with unique device categories (concatenated)
SELECT
    "Advertiser",
    STRING_AGG(distinct "DeviceCategory", ', ') AS concatenated_device_categories
FROM
    "Ad_Data_Adam"
GROUP BY
    "Advertiser";

-- 4.b list of advertisers with unique device categories (flat)
SELECT
    "Advertiser",
    "DeviceCategory"
FROM
    "Ad_Data_Adam"
GROUP BY
    "Advertiser",
    "DeviceCategory"
order by "Advertiser";


-- 5. How many duplicate records were removed by the trigger
select sum(duplicate_count) from "Ad_Data_Adam_duplicate_count";
