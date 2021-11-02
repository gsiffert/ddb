-module(attribute_test).
-include_lib("eunit/include/eunit.hrl").

encode_test_() ->
    Tests = [
        {
            "Int(42)",
            attribute:number(42),
            [{<<"N">>, <<"42">>}]
        },
        {
            "[Int(1), Int(2), Int(3)]",
            attribute:number_set([1, 2, 3]),
            [{<<"NS">>, [<<"1">>, <<"2">>, <<"3">>]}]
        },
        % {"Float(42.42)", attribute:number(42.42), [{<<"N">>, <<"4.242">>}]},
        % {"[Float(1.1), Float(2.2), Float(3.3)]", attribute:number_set([1.1, 2.2, 3.3]), [{<<"NS">>, [<<"1.1">>, <<"2.2">>, <<"3.3">>]}]}
        {
            "String(Hello)",
            attribute:string("Hello"),
            [{<<"S">>, <<"Hello">>}]
        },
        {
            "[String(Hello), String(World)]",
            attribute:string_set(["Hello", "World"]),
            [{<<"SS">>, [<<"Hello">>, <<"World">>]}]
        },
        {
            "Binary(Hello)",
            attribute:binary(<<"Hello">>),
            [{<<"B">>, <<"SGVsbG8=">>}]
        },
        {
            "[Binary(Hello), Binary(World)]",
            attribute:binary_set([<<"Hello">>, <<"World">>]),
            [{<<"BS">>, [<<"SGVsbG8=">>, <<"V29ybGQ=">>]}]
        },
        {
            "BOOL(true)",
            attribute:boolean(true),
            [{<<"BOOL">>, true}]
        },
        {
            "BOOL(false)",
            attribute:boolean(false),
            [{<<"BOOL">>, false}]
        },
        {
            "NULL",
            attribute:null(),
            [{<<"NULL">>, true}]
        },
        {
            "[Int(1), String(Hello), Binary(World)]",
            attribute:list([attribute:number(1), attribute:string("Hello"), attribute:binary(<<"World">>)]),
            [{
                <<"L">>,
                [
                    [{<<"N">>, <<"1">>}],
                    [{<<"S">>, <<"Hello">>}],
                    [{<<"B">>, <<"V29ybGQ=">>}]
                ]
            }]
        },
        {
            "Object{Name(Gaston), Age(29)}",
            attribute:object([{"Name", attribute:string("Gaston")}, {"Age", attribute:number(29)}]),
            [
                {<<"Name">>, [{<<"S">>, <<"Gaston">>}]},
                {<<"Age">>, [{<<"N">>, <<"29">>}]}
            ]
        },
        {
            "Map{Name(Gaston), Age(29)}",
            attribute:map(attribute:object([{"Name", attribute:string("Gaston")}, {"Age", attribute:number(29)}])),
            [{
                <<"M">>,
                [
                    {<<"Name">>, [{<<"S">>, <<"Gaston">>}]},
                    {<<"Age">>, [{<<"N">>, <<"29">>}]}
                ]
            }]
        },
        {
            "Complex",
            attribute:object([
                {"Profile", attribute:object([{"Name", attribute:string("Gaston")}, {"Age", attribute:number(29)}])},
                {"Languages", attribute:list([
                    attribute:object([{"Name", attribute:string("French")}, {"Level", attribute:string("Native")}]),
                    attribute:object([{"Name", attribute:string("English")}, {"Level", attribute:string("Fluent")}]),
                    attribute:object([{"Name", attribute:string("Japanese")}, {"Level", attribute:string("Beginner")}])
                ])}
            ]),
            [
                {
                    <<"Profile">>,
                    [
                        {<<"Name">>, [{<<"S">>, <<"Gaston">>}]},
                        {<<"Age">>, [{<<"N">>, <<"29">>}]}
                    ]
                },
                {
                    <<"Languages">>,
                    [
                        {<<"L">>, [
                            [
                                {<<"Name">>, [{<<"S">>, <<"French">>}]},
                                {<<"Level">>, [{<<"S">>, <<"Native">>}]}
                            ],
                            [
                                {<<"Name">>, [{<<"S">>, <<"English">>}]},
                                {<<"Level">>, [{<<"S">>, <<"Fluent">>}]}
                            ],
                            [
                                {<<"Name">>, [{<<"S">>, <<"Japanese">>}]},
                                {<<"Level">>, [{<<"S">>, <<"Beginner">>}]}
                            ]
                        ]}
                    ]
                }
            ]
        }
    ],
    [{Name, ?_assertEqual(Expected, attribute:encode(Input))} || {Name, Input, Expected} <- Tests].

decode_test_() ->
    Tests = [
        {
            "Int(42)",
            attribute:number(42),
            42
        },
        {
            "[Int(1), Int(2), Int(3)]",
            attribute:number_set([1, 2, 3]),
            [1, 2, 3]
        },
        % {"Float(42.42)", attribute:number(42.42), [{<<"N">>, <<"4.242">>}]},
        % {"[Float(1.1), Float(2.2), Float(3.3)]", attribute:number_set([1.1, 2.2, 3.3]), [{<<"NS">>, [<<"1.1">>, <<"2.2">>, <<"3.3">>]}]}
        {
            "String(Hello)",
            attribute:string("Hello"),
            "Hello"
        },
        {
            "[String(Hello), String(World)]",
            attribute:string_set(["Hello", "World"]),
            ["Hello", "World"]
        },
        {
            "Binary(Hello)",
            attribute:binary(<<"Hello">>),
            <<"Hello">>
        },
        {
            "[Binary(Hello), Binary(World)]",
            attribute:binary_set([<<"Hello">>, <<"World">>]),
            [<<"Hello">>, <<"World">>]
        },
        {
            "BOOL(true)",
            attribute:boolean(true),
            true
        },
        {
            "BOOL(false)",
            attribute:boolean(false),
            false
        },
        {
            "NULL",
            attribute:null(),
            nil
        },
        {
            "[Int(1), String(Hello), Binary(World)]",
            attribute:list([attribute:number(1), attribute:string("Hello"), attribute:binary(<<"World">>)]),
            [1, "Hello", <<"World">>]
        },
        {
            "Object{Name(Gaston), Age(29)}",
            attribute:object([{"Name", attribute:string("Gaston")}, {"Age", attribute:number(29)}]),
            [
                {"Age", 29},
                {"Name", "Gaston"}
            ]
        },
        {
            "Map{Name(Gaston), Age(29)}",
            attribute:map(attribute:object([{"Name", attribute:string("Gaston")}, {"Age", attribute:number(29)}])),
            [
                {"Age", 29},
                {"Name", "Gaston"}
            ]
        },
        {
            "Complex",
            attribute:object([
                {"Profile", attribute:object([{"Name", attribute:string("Gaston")}, {"Age", attribute:number(29)}])},
                {"Languages", attribute:list([
                    attribute:object([{"Name", attribute:string("French")}, {"Level", attribute:string("Native")}]),
                    attribute:object([{"Name", attribute:string("English")}, {"Level", attribute:string("Fluent")}]),
                    attribute:object([{"Name", attribute:string("Japanese")}, {"Level", attribute:string("Beginner")}])
                ])}
            ]),
            [
                {
                    "Languages",
                    [
                        [{"Level", "Native"}, {"Name", "French"}],
                        [{"Level", "Fluent"}, {"Name", "English"}],
                        [{"Level", "Beginner"}, {"Name", "Japanese"}]
                    ]
                },
                {
                    "Profile",
                    [
                        {"Age", 29},
                        {"Name", "Gaston"}
                    ]
                }
            ]
        }
    ],
    [{Name, ?_assertEqual(Expected, attribute:decode(attribute:encode(Input)))} || {Name, Input, Expected} <- Tests].
