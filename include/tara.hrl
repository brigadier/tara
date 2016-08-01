-record(tara_error, {code, schema, message}).
-record(tara_response, {code, schema, data}).

-define(OP_ADD(FieldNo, Arg), 		[<<"+">>, FieldNo, Arg]).
-define(OP_SUB(FieldNo, Arg), 		[<<"-">>, FieldNo, Arg]).
-define(OP_BAND(FieldNo, Arg), 		[<<"&">>, FieldNo, Arg]).
-define(OP_BXOR(FieldNo, Arg), 		[<<"^">>, FieldNo, Arg]).
-define(OP_DEL(FieldNo, Arg), 		[<<"#">>, FieldNo, Arg]).
-define(OP_INS(FieldNo, Arg),		[<<"!">>, FieldNo, Arg]).
-define(OP_ASSIGN(FieldNo, Arg), 	[<<"=">>, FieldNo, Arg]).
-define(OP_SPLICE(FieldNo, Pos, Offs, Arg), 	[<<":">>, FieldNo, Pos, Offs, Arg]).



-define(ITERATOR_EQ, 0).
-define(ITERATOR_REQ, 1).
-define(ITERATOR_ALL, 2).
-define(ITERATOR_LT, 3).
-define(ITERATOR_LE, 4).
-define(ITERATOR_GE, 5).
-define(ITERATOR_GT, 6).
-define(ITERATOR_BITSET_ALL_SET, 7).
-define(ITERATOR_BITSET_ANY_SET, 8).
-define(ITERATOR_BITSET_ALL_NOT_SET, 9).
-define(ITERATOR_OVERLAPS, 10).
-define(ITERATOR_NEIGHBOR, 11).