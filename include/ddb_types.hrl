-record(attribute, {
    type    :: string | string_set | binary | binary_set | boolean | null | number | number_set | list | map, 
    value   :: string() | nonempty_list(string()) | binary() | nonempty_list(binary()) | boolean() | true | number() | nonempty_list(number()) | nonempty_list(#attribute{}) | #{string() := #attribute{}}
}).
-record(field, {
    name        :: string(),
    attribute   :: #attribute{}
}).
