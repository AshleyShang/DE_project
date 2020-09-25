COPY(
  select *
  from retail.user_purchase
  where quantity > 2
       and cast(invoice_date as date)='{{ ds }}')
TO '{{ params.temp_filtered_user_purchase }}' WITH (FORMAT CSV, HEADER);
/* filter quantity greater than 2 and invoice in the exectution date */
