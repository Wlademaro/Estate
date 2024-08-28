from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, LongType, ArrayType

schema = StructType([
    StructField("currentPage", IntegerType(), True),
    StructField("data", ArrayType(
        StructType([
            StructField("property", StructType([
                StructField("address", StructType([
                    StructField("city", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("streetAddress", StringType(), True),
                    StructField("zipcode", StringType(), True),
                ]), True),
                StructField("bathrooms", IntegerType(), True),
                StructField("bedrooms", IntegerType(), True),
                StructField("bestGuessTimeZone", StringType(), True),
                StructField("country", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("daysOnZillow", IntegerType(), True),
                StructField("estimates", StructType([
                    StructField("rentZestimate", IntegerType(), True)
                ]), True),
                StructField("hdpView", StructType([
                    StructField("hdpUrl", StringType(), True),
                    StructField("listingStatus", StringType(), True),
                    StructField("price", IntegerType(), True)
                ]), True),
                StructField("isFeatured", BooleanType(), True),
                StructField("isPreforeclosureAuction", BooleanType(), True),
                StructField("isShowcaseListing", BooleanType(), True),
                StructField("isUnmappable", BooleanType(), True),
                StructField("lastSoldDate", LongType(), True),
                StructField("listing", StructType([
                    StructField("listingStatus", StringType(), True),
                    StructField("listingSubType", StructType([]), True)
                ]), True),
                StructField("listingDateTimeOnZillow", LongType(), True),
                StructField("livingArea", IntegerType(), True),
                StructField("location", StructType([
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True)
                ]), True),
                StructField("lotSizeWithUnit", StructType([
                    StructField("lotSize", IntegerType(), True),
                    StructField("lotSizeUnit", StringType(), True)
                ]), True),
                StructField("media", StructType([
                    StructField("allPropertyPhotos", StructType([
                        StructField("highResolution", ArrayType(StringType()), True)
                    ]), True),
                    StructField("hasApprovedThirdPartyVirtualTour", BooleanType(), True),
                    StructField("hasVRModel", BooleanType(), True),
                    StructField("hasVideos", BooleanType(), True),
                    StructField("propertyPhotoLinks", StructType([
                        StructField("highResolutionLink", StringType(), True)
                    ]), True),
                    StructField("thirdPartyPhotoLinks", StructType([
                        StructField("satelliteLink", StringType(), True),
                        StructField("streetViewLink", StringType(), True),
                        StructField("streetViewMetadataLink", StringType(), True)
                    ]), True)
                ]), True),
                StructField("personalizedResult", StructType([
                    StructField("isViewed", BooleanType(), True),
                    StructField("userRecommendation", StructType([
                        StructField("isRecommendedForYou", BooleanType(), True)
                    ]), True)
                ]), True),
                StructField("price", StructType([
                    StructField("pricePerSquareFoot", IntegerType(), True),
                    StructField("value", IntegerType(), True)
                ]), True),
                StructField("propertyDisplayRules", StructType([
                    StructField("agent", StructType([]), True),
                    StructField("builder", StructType([]), True),
                    StructField("canShowAddress", BooleanType(), True),
                    StructField("canShowOnMap", BooleanType(), True),
                    StructField("listingCategory", StringType(), True),
                    StructField("mls", StructType([
                        StructField("brokerName", StringType(), True),
                        StructField("mustDisplayBrokerName", BooleanType(), True)
                    ]), True),
                    StructField("providerLogo", StringType(), True),
                    StructField("soldByOffice", StructType([]), True)
                ]), True),
                StructField("propertyType", StringType(), True),
                StructField("region", StructType([]), True),
                StructField("rental", StructType([
                    StructField("areApplicationsAccepted", BooleanType(), True)
                ]), True),
                StructField("ssid", IntegerType(), True),
                StructField("taxAssessment", StructType([
                    StructField("taxAssessedValue", IntegerType(), True),
                    StructField("taxAssessmentYear", StringType(), True)
                ]), True),
                StructField("yearBuilt", IntegerType(), True),
                StructField("zillowOwnedProperty", StructType([
                    StructField("isZillowOwned", BooleanType(), True)
                ]), True),
                StructField("zpid", LongType(), True)
            ]), True),
            StructField("resultType", StringType(), True)
        ])
    ), True),
    StructField("message", StringType(), True),
    StructField("resultsPerPage", IntegerType(), True),
    StructField("status", BooleanType(), True),
    StructField("totalPages", IntegerType(), True),
    StructField("totalResultCount", IntegerType(), True),
    StructField("type", StringType(), True)
])