const targetLTSVersion = "12.13.1";

module.exports = function (api) {  
  let isProduction = api.env(["production"]); 

  return {   
    "env": { 
      "development": {
        "sourceMaps": "inline",
        "plugins": ["source-map-support"]
      },
      "production": {
        "minified": true        
      }      
    },  
    "presets": [
      [
        "@babel/env",
        {      
          "debug": !isProduction,
          "targets": {     
            "node": isProduction ? targetLTSVersion : "current" 
          },
          "exclude": [             
            "@babel/plugin-transform-regenerator" 
          ]
        }
      ]
    ],
    "comments": false,
    "ignore": [
      "node_modules",      
      "src/**/__test__"
    ], 
    "plugins": [
      ["contract", {        
        "strip": isProduction,
        "envStrip": true
      }],      
      ["@babel/plugin-proposal-decorators", {"legacy": true}],
      ["@babel/plugin-proposal-class-properties", { "loose": true }],
      "@babel/plugin-proposal-nullish-coalescing-operator",
      "@babel/plugin-proposal-optional-chaining",
      "@babel/plugin-proposal-logical-assignment-operators",
      ["@babel/plugin-proposal-pipeline-operator", { "proposal": "minimal" }],
      "@babel/plugin-proposal-partial-application"
    ]
  };
}