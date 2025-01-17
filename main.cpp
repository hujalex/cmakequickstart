#include <arrow/array/array_base.h>
#include <arrow/chunked_array.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>
#include <iostream>
#include <arrow/api.h>
#include <memory>

using namespace std;

arrow::Status buildArrays(
    arrow::Int8Builder & int8builder,
    arrow::Int16Builder & int16builder,
    std::shared_ptr<arrow::Array>& days, std::shared_ptr<arrow::Array>& months, std::shared_ptr<arrow::Array>& years) {

    int8_t days_raw[5] = {1,12,17,23,28};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 5));
    ARROW_ASSIGN_OR_RAISE(days, int8builder.Finish()); // state resets so can be used again

    int8_t months_raw[5] = {1,3,5,7,1};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, 5));
    ARROW_ASSIGN_OR_RAISE(months, int8builder.Finish());

    int16_t years_raw[5] = {1990,2000, 1995, 2000, 1995};
    ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw, 5));
    ARROW_ASSIGN_OR_RAISE(years, int16builder.Finish());

    return arrow::Status::OK();
}


arrow::Status buildChunkedArray(
arrow::Int8Builder & int8builder,
arrow::Int16Builder & int16builder,
std::shared_ptr<arrow::Array> & days2,
std::shared_ptr<arrow::Array> & months2,
std::shared_ptr<arrow::Array> & years2) {
    int8_t days_raw2[5] = {6, 12, 3, 30, 22};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw2, 5));
    ARROW_ASSIGN_OR_RAISE(days2, int8builder.Finish());

    int8_t months_raw2[5] = {5, 4, 11, 3, 2};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw2, 5));
    ARROW_ASSIGN_OR_RAISE(months2, int8builder.Finish());

    int16_t years_raw2[5] = {1980, 2001, 1915, 2020, 1996};
    ARROW_RETURN_NOT_OK(int16builder. AppendValues(years_raw2, 5));
    ARROW_ASSIGN_OR_RAISE(years2, int16builder.Finish());

    return arrow::Status::OK();
}

void defineSchema(
std::shared_ptr<arrow::Array> & days, 
std::shared_ptr<arrow::Array> & months,
std::shared_ptr<arrow::Array> & years
) {
    std::shared_ptr<arrow::Field> field_day, field_month, field_year;
    std::shared_ptr<arrow::Schema> schema;

    field_day = arrow::field("Day", arrow::int8());
    field_month = arrow::field("Month", arrow::int8());
    field_year = arrow::field("Year", arrow::int16());

    schema = arrow::schema({field_day, field_month, field_year});

    std::shared_ptr<arrow::RecordBatch> rbatch;
    //The RecordBatch needs the schema,, length for columns, data
    rbatch = arrow::RecordBatch::Make(schema, days->length(), {days, months, years});
    std::cout << rbatch->ToString();
}

 buildArrayVector(
    std::shared_ptr<arrow::Array> & days,
    std::shared_ptr<arrow::Array> & days2,
    std::shared_ptr<arrow::Array> & months,
    std::shared_ptr<arrow::Array> & months2,
    std::shared_ptr<arrow::Array> & years,
    std::shared_ptr<arrow::Array> & years2
) {
    arrow::ArrayVector day_vecs{days, days2};
    std::shared_ptr<arrow::ChunkedArray> day_chunks = 
        std::make_shared<arrow::ChunkedArray>(day_vecs);
    
    arrow::ArrayVector month_vecs{months, months2};
    std::shared_ptr<arrow::ChunkedArray> month_chunks = 
        std::make_shared<arrow::ChunkedArray>(month_vecs);

    arrow::ArrayVector year_vecs{years, years2};
    std::shared_ptr<arrow::ChunkedArray> year_chunks = 
        std::make_shared<arrow:ChunkedArray>(year_vecs);


    return arrow::Status::OK();
}

arrow::Status RunMain() {
    // Implement the function logic here
    arrow::Int8Builder int8builder;
    arrow::Int16Builder int16builder;
    std::shared_ptr<arrow::Array> days, months, years;
    std::shared_ptr<arrow::Array> days2, months2, years2;

    ARROW_RETURN_NOT_OK(buildChunkedArray(int8builder, int16builder, days2, months2, years2));
    ARROW_RETURN_NOT_OK(buildArrays(int8builder, int16builder, days, months, years));
    ARROW_RETURN_NOT_OK(buildArrayVector(days, days2, months, months2, years, years2));

    defineSchema(days, months, years);


    return arrow::Status::OK();
}



int main() {
    arrow::Status st = RunMain();
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    return 0;
}   