/* This is sample function file, modify it with required values. Remove the descriptions/comments to make it JSON file.*/
[
	{
		'functionName'	: 'lookupMongoDB',                         //Name of the function
		'functionClass'	: 'com.streamanalytix.extensions.lookup.MongoCustomUdf',    //Fully qualified name of function implementation class
		'argsNames'	    : 'param1,param2',                         //Comma separated function arguments
		'returnType'	: 'java.lang.String',                      //Function return type
		'functionDesc'	: 'To verifying template functionality',   //function description.
		
		'parameters'	: [
			{
				"propertyName": {
					"type": "label",            //label of the input parameter
					"value": "text-fld"         //Name of the input parameter
				},
				"propertyValue": {
					"type": "text",            //Type of the input parameter
					"value": "",               //Value of the input parameter, if any predefined value or leave it blank.
					"required": true           // Possible values [true, false]
				}
			},
			{
				"propertyName": {
					"type": "label",
					"value": "password-fld"
				},
				"propertyValue": {
					"type": "password",
					"value": "",
					"required": true
				}
			},
			{
				"propertyName": {
					"type": "label",
					"value": "email-fld"
				},
				"propertyValue": {
					"type": "email",
					"value": "",
					"required": true
				}
			},
			{
				"propertyName": {
					"type": "label",
					"value": "url-fld"
				},
				"propertyValue": {
					"type": "url",
					"value": "",
					"required": true
				}
			},
			{
				"propertyName": {
					"type": "label",
					"value": "number-fld"
				},
				"propertyValue": {
					"type": "number",
					"value": "",
					"required": true
				}
			},
			{
				"propertyName": {
					"type": "label",
					"value": "integer-fld"
				},
				"propertyValue": {
					"type": "integer",
					"value": "",
					"required": true
				}
			},
			{
				"propertyName": {
					"type": "label",
					"value": "alphanum-fld"
				},
				"propertyValue": {
					"type": "alphanum",
					"value": "",
					"required": true
				}
			},
			{
				"propertyName": {
					"type": "label",
					"value": "checkbox-fld"
				},
				"propertyValue": {
					"type": "checkbox",
					"options": [
						{
							"label": "Enable",
							"value": "enable"
						},
						{
							"label": "Disable",
							"value": "disable"
						}
					],
					"value": "enable",
					"required": true
				}
			},
			{
				"propertyName": {
					"type": "label",
					"value": "radio-fld"
				},
				"propertyValue": {
					"type": "radio",
					"options": [
						{
							"label": "Male",
							"value": "male"
						},
						{
							"label": "Female",
							"value": "female"
						}
					],
					"value": "female",
					"required": true
				}
			},
			{
				"propertyName": {
					"type": "label",
					"value": "select-fld"
				},
				"propertyValue": {
					"type": "select",
					"options": [
						{
							"label": "One",
							"value": "one"
						},
						{
							"label": "Two",
							"value": "two"
						},
						{
							"label": "Three",
							"value": "three"
						}
					],
					"value": "two",
					"required": true
				}
			},
			{
				"propertyName": {
					"type": "label",
					"value": "textarea-fld"
				},
				"propertyValue": {
					"type": "textarea",
					"value": "",
					"required": true
				}
			}
		]
	}
	
]