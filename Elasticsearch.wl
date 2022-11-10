(* ::Package:: *)

(* ::Title:: *)
(*Elasticsearch utilities*)


BeginPackage["Elasticsearch`"]


(* ::Section:: *)
(*Usage help - symbol declarations*)


(* ::Subsection:: *)
(*Call management*)


ESClient::usage="ESClient[options...] - returns a client object with basic access settings. See Options[ESClient] for defaults"


ESCall::usage="ESCall[client, method, path, options...] - is a general HTTP call based on the passed client and additional options given on the command line"


ESPost::usage="ESPost[client, path, body] - a simplified POST request. The body is an association that will be converted to the actual JSON payload required by ES"


ESGet::usage="ESGet[client, path] - simplified GET request"


ESScrollStart::usage="ESScrollStart[client, path, body, timeout] - Run a search query as you would with ESPost - the timeout defaults to 1m"


ESScrollId::usage="ESScrollId[res] - extracts the scroll id from a ESScrollStart call"


ESScrollNext::usage="ESScrollNext[client, scrollId, timeout] - pull the next set of data in the scroll - timeout defaults to 1m"


(* ::Subsection:: *)
(*Queries*)


ESQuery::usage="ESQuery[query] - define the (top-level) query portion of a search"


ESBoolQuery::usage="ESBoolQuery[boolGroups] - combine multiple query types"


ESBoolMust::usage ="ESBoolMust[conditions] - bool query condition must be satisfied"


ESBoolMustNot::usage ="ESBoolMustNot[conditions] - bool query condition must not be satisfied"


ESBoolShould::usage ="ESBoolMust[conditions] - bool query condition 'should' (or) be satisfied"


ESMatchAllQuery::usage ="ESMatchAllQuery[] - just that"


ESTermQuery::usage="ESTermQuery[field, value] - limit to documents where field=value"


ESTermsQuery::usage="ESTermsQuery[field, list] - shape a query for documents where 'field' matches the elements in 'list'"


ESRangeQuery::usage="ESRangeQuery[timestampField,from,to] - limit query to the range specified"


ESPrefixQuery::usage="ESPrefixQuery[field,prefix] - limit the query to documents where 'field' starts with 'prefix'"


(* ::Subsection:: *)
(*Aggregations*)


ESAggs::usage="ESAggs[aggs] - top level aggregation component"


ESSubAggs::usage="ESSubAggs[agg, subAggs] - adds a sub aggregation to an existing aggregation and returns the full aggregation"


ESCompositeAggregation::usage="ESCompositeAggregation[label, fields, afterKey, chunkSize] - set up a paginated aggregation for documents that have the same values for the fields provided. It is sufficient to provide only the two first parameters when setting up a query. Subsequent queries will want 'afterKey' as provided in the response to start at the right spot."


ESGetCompositeAfterKey::usage="ESGetCompositeAfterKey[resp,label] - pulls out the key to start the next aggregation on. Returns Null if there is no such key (end of the aggregate)."


ESGetAggregation::usage="ESGetAggregation[resp,label] - returns the requested aggregation from a response."


ESFilterAggregation::usage="ESFilterAggregation[label, field, value] - filter aggregation results (term like) on documents that has field=value."


ESCardinalityAggregation::usage="ESCardinalityAggregation[label,field] - count (estimate) the number of unique instances of 'field' in the reponse."


ESSumAggregation::usage="ESSumAggregation[label,field] - sum the values of 'field'"


ESDateHistogramAggregation::usage="ESDateHistogramAggregation[label, field, interval, timeZone] - bin counts into a date histogram"


ESTermsAggregation::usage="ESTermsAggregation[label, field, size] - return the top 'size' documents based on the value in 'field'"


ESMaxAggregation::usage="ESMaxAggregation[label, field] - return the largest value of 'field' in the search"


ESMinAggregation::usage="ESMinAggregation[label, field] - return the smallest value of 'field' in the search"


ESValueCountAggregation::usage="ESValueCountAggregation[label,field] - count the number of documents which define this value"


ESAvgAggregation::usage="ESAvgAggregation[label,field] - mean of the specified field"


(* ::Subsection:: *)
(*Miscellaneous*)


ESSize::usage="ESSize[size] - provide return size for query or aggregation"


ToJSON::usage="ToJSON[association] - exports the given association to JSON"


ESGetAggregation::usage="ESGetAggregation[resp,label] - returns the requested aggregation from a response."


(* ::Section:: *)
(*Call definitions*)


Begin["`Private`"]


(* ::Subsection:: *)
(*Call management*)


Options[ESClient]={
	"Domain"->"localhost",
	"Port"->9200,
	"Scheme"->"http",
	"ContentType"->"application/json",
	"Headers"->{"Accept"->"application/json"},
	"Username"->"", (* Username and Password not used currently *)
	"Password"->"",
	"VerifySecurityCertificates"->True
};


ESClient[opts:OptionsPattern[]]:=<|Join[Options[ESClient],List[opts]]|>/.Association->List


ESCall[client_, method_, path_, opts:OptionsPattern[]]:=Module[{req,combined,auth},
	combined=<|Join[client,List[opts]]|>/.Association->List;
	req=HTTPRequest[path,
		Append[
			<|Method->ToUpperCase[method]|>,
			FilterRules[combined,{"Domain","Port","Scheme","Query","Body","ContentType","Headers","Username","Password"}]
		], 
		FilterRules[combined,Options[HTTPRequest]]/.List->Sequence
	];
	auth:=<|"Username"->("Username"/.client),"Password"->("Password"/.client)|>;
	URLExecute[req,{},"RawJSON"]
]


ESPost[ec_,path_,body_]:=ESCall[ec,"POST",path,"Body"->ExportString[body,"JSON"]]


ESGet[ec_,path_]:=ESCall[ec,"GET",path]


(* ::Input::Initialization:: *)
ESScrollStart[client_,path_,body_,timeout_:"1m"]:= ESPost[client,path<>"?scroll="<>timeout,body]


(* ::Input::Initialization:: *)
ESScrollId[res_]:=res["_scroll_id"]


(* ::Input::Initialization:: *)
ESScrollNext[client_,sid_,timeout_:"1m"]:=ESPost[client,"/_search/scroll",<|"scroll"->timeout,"scroll_id"->sid|>]


(* ::Subsection:: *)
(*Queries*)


ESQuery[query__Rule]:="query"-><|query|>


ESBoolQuery[boolGroups__Rule]:="bool"-><|boolGroups|>


ESBoolMust[conditions:{__Rule}]:="must"->(<|#|>&/@conditions)


ESBoolMustNot[conditions:{__Rule}]:="must_not"->(<|#|>&/@conditions)


ESBoolShould[conditions:{__Rule}]:="should"->(<|#|>&/@conditions)


ESMatchAllQuery[]:="match_all"-><||>


ESTermQuery[field_,value_]:="term"-><|field->value|>


ESTermsQuery[field_,list_]:="terms"-><|field->list|>


ESRangeQuery[timestampField_,from_,to_]:="range"-><|
	timestampField-><|
		"from"->from, 
		"to"->to, 
		"include_lower"->True,
		"include_upper"->False
	|>
|>


ESPrefixQuery[field_,prefix_]:="prefix"-><|field-><|"value"->prefix|>|>


(* ::Subsection:: *)
(*Aggregation*)


ESAggs[aggs__Rule]:="aggs"-><|aggs|> 


ESTermsAggregation[label_,field_,size_Integer]:=label-><|"terms"-><|"field"->field, "size"->size|>|>


ESSubAggs[agg_Rule, subAggs__Rule]:=Module[{label},
	label=agg[[1]];
	label->Append[agg[[2]],subAggs]
]


ESCompositeAggregation[label_,fields_List,afterKey_:Null,chunkSize_:10]:=
	label-><|
		"composite"-><|
			"size"->chunkSize,
			"after"->If[afterKey===Null,<|#->""&/@fields|>,afterKey],
			"sources"->(<|#-><|"terms"-><|"field"->#|>|>|>&/@fields)
		|>
	|>


ESGetCompositeAfterKey[resp_, label_]:=If[
	KeyExistsQ[resp["aggregations"][label],"after_key"],
	resp["aggregations"][label]["after_key"],
	Null
]


ESGetAggregation[resp_, label_]:=resp["aggregations"][label]["buckets"]


ESFilterAggregation[label_,field_,value_]:=label-><|"filter"-><|"term"-><|field->value|>|>|>


ESCardinalityAggregation[label_,field_]:=label-><|
	"cardinality"-><|"field"->field|>
|>


ESSumAggregation[label_,field_]:=label-><|"sum"-><|"field"->field|>|>


ESDateHistogramAggregation[label_,field_,interval_,timeZone_]:=label-><|
	"date_histogram"-><|
		"field"->field,
		"interval"->interval,
		"time_zone"->timeZone
	|>
|>


ESTermsAggregation[label_,field_,size_]:=label-><|
	"terms"-><|"field"->field, "size"->size|>
|>


ESMaxAggregation[label_, field_]:=label-><|
	"max"-><|"field"->field|>
|>


ESMinAggregation[label_, field_]:=label-><|
	"min"-><|"field"->field|>
|>


ESValueCountAggregation[label_,field_]:=label-><|"value_count" -> <|"field"->field|>|>


ESAvgAggregation[label_,field_]:=label-><|
	"avg"-><|"field"->field|>
|>


(* ::Subsection:: *)
(*Miscellaneous*)


ESSize[n_Integer]:="size"->n


ToJSON[assoc_]:=ExportString[assoc,"JSON"]


(* ::Section:: *)
(*Closure*)


End[]


EndPackage[]
