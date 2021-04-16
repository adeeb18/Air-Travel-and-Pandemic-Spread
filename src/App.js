import React, { Component } from 'react';
import {Bar, Line} from 'react-chartjs-2';
// import Chart from 'chart.js';
import { BrowserRouter, Route, Switch } from 'react-router-dom';
import socketIOClient from "socket.io-client";
const ENDPOINT = "http://127.0.0.1:5000";

/*
var ctx = 'myChart';
const config = {
  type: 'line',
  datasets: {
    labels: [],
    data: [{}]
  },
  options: {
    responsive: true,
    plugins: {
      legend: {
        position: 'top',
      },
      title: {
        display: true,
        text: 'Chart.js Line Chart'
      }
    }
  }
};
var graph = new Chart(ctx, config);
*/


const info = {
  labels: [],
  datasets: [
    {
      
    }
  ]
}

const info2 = {
  labels: [],
  datasets: [
    {
      
    }
  ]
}

const info3 = {
  labels: [],
  datasets: [
    {
      
    }
  ]
}

class App extends Component {
  constructor(props) {
    super(props);
    this.state = {
      list: [], // Input from searchbars
      socket: socketIOClient(ENDPOINT), // Socket (duh)
      labels: [], // x-axis stuff
      datasets: [], // Info about graph (data: is the actual data)
      updated: false, // Whether the graph has been displayed
      opts: {}, // Options
      labels2: [],
      dataset2: [],
      opts2: {},
      labels3: [],
      dataset3: [],
      opts3: {}
    };
    // this.buttonClick = this.buttonClick.bind(this);
    this.addItem1 = this.addItem1.bind(this);
    this.addItem2 = this.addItem2.bind(this);
    this.addItem3 = this.addItem3.bind(this);
    this.addItem4 = this.addItem4.bind(this);
    this.addItem5 = this.addItem5.bind(this);
  }

  addItem1(e) {
    // Prevent button click from submitting form
    e.preventDefault();

    // Create variables for our list, the item to add, and our form
    let list = this.state.list;
    const newItem1 = document.getElementById("search1");
    const newItem2 = document.getElementById("search2");
    const newItem3 = document.getElementById("search3");
    const form = document.getElementById("addItemForm");

    // If our input has a value
    if (newItem1.value !== "" && newItem2.value !== "" && newItem3.value !== "") {
      // Add the new item to the end of our list array
      list.push(newItem1.value);
      list.push(newItem2.value);
      list.push(newItem3.value);
      // Then we use that to set the state for list
      this.setState({
        list: list
      });

      console.log("Ran 1");
      this.state.socket.emit("Query 1",this.state.list);

      // Finally, we need to reset the form
      newItem1.classList.remove("is-danger");
      newItem2.classList.remove("is-danger");
      newItem3.classList.remove("is-danger");
      form.reset();
    } else {
      // If the input doesn't have a value, make the border red since it's required
      newItem1.classList.add("is-danger");
      newItem2.classList.add("is-danger");
      newItem3.classList.add("is-danger");
    }
  }

  addItem2(e) {
    // Prevent button click from submitting form
    e.preventDefault();

    // Create variables for our list, the item to add, and our form
    let list = this.state.list;
    const newItem = document.getElementById("search");
    const form = document.getElementById("addItemForm");

    // If our input has a value
    if (newItem.value !== "") {
      // Add the new item to the end of our list array
      list.push(newItem.value);
      // Then we use that to set the state for list
      this.setState({
        list: list
      });

      this.state.socket.emit("Query 2", this.state.list);

      // Finally, we need to reset the form
      newItem.classList.remove("is-danger");
      form.reset();
    } else {
      // If the input doesn't have a value, make the border red since it's required
      newItem.classList.add("is-danger");
    }
  }

  addItem3(e) {
    // Prevent button click from submitting form
    e.preventDefault();

    // Create variables for our list, the item to add, and our form
    let list = this.state.list;
    const newItem = document.getElementById("search");
    const form = document.getElementById("addItemForm");

    // If our input has a value
    if (newItem.value !== "") {
      // Add the new item to the end of our list array
      list.push(newItem.value);
      // Then we use that to set the state for list
      this.setState({
        list: list
      });

      console.log("Ran 3");
      this.state.socket.emit("Query 3",this.state.list);

      // Finally, we need to reset the form
      newItem.classList.remove("is-danger");
      form.reset();
    } else {
      // If the input doesn't have a value, make the border red since it's required
      newItem.classList.add("is-danger");
    }
  }

  addItem4(e) {
    // Prevent button click from submitting form
    e.preventDefault();

    // Create variables for our list, the item to add, and our form
    let list = this.state.list;
    const newItem1 = document.getElementById("search1");
    const newItem2 = document.getElementById("search2");
    const newItem3 = document.getElementById("search3");
    const form = document.getElementById("addItemForm");

    // If our input has a value
    if (newItem1.value !== "" && newItem2.value !== "" && newItem3.value !== "") {
      // Add the new item to the end of our list array
      list.push(newItem1.value);
      list.push(newItem2.value);
      list.push(newItem3.value);
      // Then we use that to set the state for list
      this.setState({
        list: list
      });

      console.log("Ran 4");
      this.state.socket.emit("Query 4",this.state.list);

      // Finally, we need to reset the form
      newItem1.classList.remove("is-danger");
      newItem2.classList.remove("is-danger");
      newItem3.classList.remove("is-danger");
      form.reset();
    } else {
      // If the input doesn't have a value, make the border red since it's required
      newItem1.classList.add("is-danger");
      newItem2.classList.add("is-danger");
      newItem3.classList.add("is-danger");
    }
  }

  addItem5(e) {
    // Prevent button click from submitting form
    e.preventDefault();

    // Create variables for our list, the item to add, and our form
    let list = this.state.list;
    const newItem = document.getElementById("search");
    const form = document.getElementById("addItemForm");

    // If our input has a value
    if (newItem.value !== "") {
      // Add the new item to the end of our list array
      list.push(newItem.value);
      // Then we use that to set the state for list
      this.setState({
        list: list
      });

      console.log("Ran");
      this.state.socket.emit("Query 5",this.state.list);

      // Finally, we need to reset the form
      newItem.classList.remove("is-danger");
      form.reset();
    } else {
      // If the input doesn't have a value, make the border red since it's required
      newItem.classList.add("is-danger");
    }
  }

  componentDidMount() {
    this.state.socket.on('Query 1',data => {
      if(!this.state.updated) {
        console.log(data);
        var l = data.length;
        var N1 = [];
        var ND1 = [];
        var N2 = [];
        var ND2 = [];
        var N3 = [];
        var ND3 = [];
        var sum1 = [];
        var sum2 = [];
        var sum3 = [];
        var li = [];

        sum1.push(data[100]["DBP1"]);
        sum2.push(data[100]["DBP2"]);
        sum3.push(data[100]["DBP3"]);
        sum1.push(data[100]["S1"]);
        sum2.push(data[100]["S2"]);
        sum3.push(data[100]["S3"]);
        sum1.push(data[100]["A1"]);
        sum2.push(data[100]["A2"]);
        sum3.push(data[100]["A3"]);
        sum1.push(data[100]["HW1"]);
        sum2.push(data[100]["HW2"]);
        sum3.push(data[100]["HW3"]);

        var str1 = this.state.list[0];
        var str2 = this.state.list[1];
        var str3 = this.state.list[2];

        for(var i = 0; i < l; i++) {
          li.push(data[i]["X"]);
          N1.push(data[i]["N1"]);
          ND1.push(data[i]["N1"]);
          N2.push(data[i]["N2"]);
          ND2.push(data[i]["ND2"]);
          N3.push(data[i]["N3"]);
          ND3.push(data[i]["ND3"]);
        }
        
        console.log(li);
        console.log(ND1);
        console.log(ND2);
        console.log(ND3);
        console.log(sum1);
        console.log(sum2);
        console.log(sum3);


        this.setState({
          labels: li,
          datasets: [{
            fill: false,
            label: str1.concat(' Cases'),
            lineTension: 0.5,
            borderColor: 'rgba(22,150,210)',
            borderWidth: 1,
            data: N1
          },
          {
            fill: false,
            label: str2.concat(' Cases'),
            lineTension: 0.5,
            borderColor: 'rgba(220,20,60)',
            borderWidth: 1,
            data: N2
          },
          {
            fill: false,
            label: str3.concat(' Cases'),
            lineTension: 0.5,
            borderColor: 'rgba(0,0,1)',
            borderWidth: 1,
            data: N3
          }],

          opts: {
            title:{
              display:true,
              text:'New Cases for '.concat(str1).concat(', ').concat(str2).concat(', and ').concat(str3),
              fontSize:20
            },
            animation: {
              duration: 0
            },
            hover: {
              animationDuration: 0
            },
            responsiveAnimationDuration: 0,
            legend: {
              display:true
            }
          },

          labels2: li,

          dataset2: [{
            fill: false,
            label: str1.concat(' Deaths'),
            lineTension: 0.5,
            borderColor: 'rgba(22,150,210)',
            borderWidth: 1,
            data: ND1
          },
          {
            fill: false,
            label: str2.concat(' Deaths'),
            lineTension: 0.5,
            borderColor: 'rgba(220,20,60)',
            borderWidth: 1,
            data: ND2
          },
          {
            fill: false,
            label: str3.concat(' Deaths'),
            lineTension: 0.5,
            borderColor: 'rgb(0,0,1)',
            borderWidth: 1,
            data: ND3
          }],

          opts2: {
            title:{
              display:true,
              text:'New Deaths for '.concat(str1).concat(', ').concat(str2).concat(', and ').concat(str3),
              fontSize:20
            },
            animation: {
              duration: 0
            },
            hover: {
              animationDuration: 0
            },
            responsiveAnimationDuration: 0,
            legend: {
              display:true
            }
          },

          labels3: ["Diabetes Rate", "Smoking Rate", "Age Rate over 60", "Hand Washing Station Prevalence"],

          datasets3: [{
            label: str1,
            backgroundColor: 'rgba(65,105,225)',
            data: sum1,
          },
          {
            label: str2,
            backgroundColor: 'rgba(220,20,60)',
            data: sum2,
          },
          {
            label: str3,
            backgroundColor: 'rgba(34,139,34)',
            data: sum3,
          }],

          opts3: {
            title:{
              display:true,
              text:'Summary statistics for '.concat(str1).concat(', ').concat(str2).concat(', and ').concat(str3),
              fontSize:20
            },
            animation: {
              duration: 0
            },
            hover: {
              animationDuration: 0
            },
            responsiveAnimationDuration: 0,
            legend: {
              display:true
            }
          }
        });

        console.log(this.state.datasets3);

        info["labels"] = this.state.labels;
        info["datasets"] = this.state.datasets;
        info2["labels"] = this.state.labels2;
        info2["datasets"] = this.state.dataset2;
        info3["labels"] = this.state.labels3;
        info3["datasets"] = this.state.datasets3;
        console.log(info3);

        setInterval(() => {}, 10000);

        this.setState({
          updated: true
        });
      }
    });

    this.state.socket.on('Query 2',data => {
      if(!this.state.updated) {
        console.log(data);
        var l = data.length;
        var temp1 = [];
        var temp2 = [];
        var temp3 = [];
        var temp4 = [];
        var str = this.state.list[0];

        for(var i = 0; i < l; i++) {
          temp1.push(data[i]["Y1"]);
          temp2.push(data[i]["Y2"]);
          temp3.push(data[i]["Y3"]);
          temp4.push(data[i]["Y4"]);
        }
        
        console.log(temp1);
        console.log(temp2);
        console.log(temp3);
        console.log(temp4);

        this.setState({
          labels: ["February", "March", "April", "May", "June","July", "August",
                  "September", "October", "November"],
          datasets: [{
            fill: false,
            label:"Flights",
            yAxisID: 'A',
            lineTension: 0.5,
            borderColor: 'rgba(65,105,225)',
            borderWidth: 2,
            data: temp1
          },
          {
            fill: false,
            label:"Covid Cases",
            yAxisID: 'B',
            lineTension: 0.5,
            borderColor: 'rgba(220,20,60)',
            borderWidth: 2,
            data: temp2
          }],

          opts: {
            title:{
              display:true,
              text:'Flights and Covid Cases per Month for '.concat(str),
              fontSize:20
            },
            scales: {
              yAxes: [{
                id: 'A',
                type: 'linear',
                position: 'left',
              }, {
                id: 'B',
                type: 'linear',
                position: 'right',
              }]
            },
            animation: {
              duration: 0
            },
            hover: {
              animationDuration: 0
            },
            responsiveAnimationDuration: 0,
            legend: {
              display:true
            }
          },

          labels2: ["February", "March", "April", "May", "June","July", "August",
                  "September", "October", "November"],

          datasets2: [{
            fill: false,
            label:"Normalized Flights",
            yAxisID: 'A',
            lineTension: 0.5,
            borderColor: 'rgba(65,105,225)',
            borderWidth: 2,
            data: temp3
          },
          {
            fill: false,
            label:"Normalized covid cases",
            yAxisID: 'B',
            lineTension: 0.5,
            borderColor: 'rgba(220,20,60)',
            borderWidth: 2,
            data: temp4
          }],

          opts2: {
            title:{
              display:true,
              text:'Flights and Covid Cases per Month for '.concat(str).concat(' Normalized for Population'),
              fontSize:20
            },
            scales: {
              yAxes: [{
                id: 'A',
                type: 'linear',
                position: 'left',
              }, {
                id: 'B',
                type: 'linear',
                position: 'right',
              }]
            },
            animation: {
              duration: 0
            },
            hover: {
              animationDuration: 0
            },
            responsiveAnimationDuration: 0,
            legend: {
              display:true
            }
          }
        });

        info["labels"] = this.state.labels;
        info["datasets"] = this.state.datasets;
        info2["labels"] = this.state.labels2;
        info2["datasets"] = this.state.datasets2;

        setInterval(() => {}, 10000);

        this.setState({
          updated: true
        });
      }
    });

    this.state.socket.on('Query 3',data => {
      if(!this.state.updated) {
        var l = data.length;
        var flights = [];
        var cases = [];
        var brazil = [];
        var china = [];
        var india = [];
        var unitedstates = [];
        var str = this.state.list[0];

        for(var i = 0; i < l; i++) {
          flights.push(data[i]["FLIGHTS"]);
          cases.push(data[i]["CASES"]);
          brazil.push(data[i]["BRAZIL"]);
          china.push(data[i]["CHINA"]);
          india.push(data[i]["INIDA"]);
          unitedstates.push(data[i]["UNITEDSTATES"]);
        }
        
        console.log(cases);

        this.setState({
          labels: ["February", "March", "April", "May", "June","July", "August",
                  "September", "October", "November"],
          datasets: [{
            fill: false,
            label: 'Flights',
            yAxisID: 'A',
            lineTension: 0.5,
            borderColor: 'rgba(65,105,225)',
            borderWidth: 2,
            data: flights
          },
          {
            fill: false,
            label: 'Cases',
            yAxisID: 'B',
            lineTension: 0.5,
            borderColor: 'rgba(220,20,60)',
            borderWidth: 2,
            data: cases
          }],

          opts: {
            title:{
              display:true,
              text:'New Covid Cases in '.concat(str).concat(' and Flights Into ').concat(str).concat(' From COVID Hotspots'),
              fontSize:20
            },
            scales: {
              yAxes: [{
                id: 'A',
                type: 'linear',
                position: 'left',
              }, {
                id: 'B',
                type: 'linear',
                position: 'right',
              }]
            },
            animation: {
              duration: 0
            },
            hover: {
              animationDuration: 0
            },
            responsiveAnimationDuration: 0,
            legend: {
              display:true
            }
          },

          labels2: ["February", "March", "April", "May", "June","July", "August",
          "September", "October", "November"],
          dataset2: [{
            label: 'Brazil',
            backgroundColor: 'rgba(65,105,225)',
            data: brazil,
            stack: '0'
          },
          {
            label: 'India',
            backgroundColor: 'rgba(220,20,60)',
            data: india,
            stack: '0'
          },
          {
            label: 'China',
            backgroundColor: 'rgba(34,139,34)',
            data: china,
            stack: '0'
          },
          {
            label: 'United States',
            backgroundColor: 'rgba(0, 0, 1)',
            data: unitedstates,
            stack: '0'
          }],

          opts2: {
            title:{
              display:true,
              text:'Flights into '.concat(str).concat(' per Month per Hotspot'),
              fontSize:20
            },
            scales: {
              x: {
                  stacked: true
              },
              y: {
                  stacked: true
              }
            },
            animation: {
              duration: 0
            },
            hover: {
              animationDuration: 0
            },
            responsiveAnimationDuration: 0,
            legend: {
              display:true
            }
          },
        });

        console.log(this.state.dataset2);

        info["labels"] = this.state.labels;
        info["datasets"] = this.state.datasets;

        info2["labels"] = this.state.labels2;
        info2["datasets"] = this.state.dataset2;

        setInterval(() => {}, 10000);

        this.setState({
          updated: true
        });
      }
    });

    this.state.socket.on('Query 4',data => {
      console.log("Starting Graphing 4");
      if(!this.state.updated) {
        var l = data.length;
        var temp1 = [];
        var temp2 = [];
        var temp3 = [];
        var str1 = this.state.list[0];
        var str2 = this.state.list[1];
        var str3 = this.state.list[2];

        for(var i = 0; i < l; i++) {
          temp1.push(data[i]["Y1"]);
          temp2.push(data[i]["Y2"]);
          temp3.push(data[i]["Y3"]);
        }
        
        console.log(temp1);
        console.log(temp2);
        console.log(temp3);

        this.setState({
          labels: ["February", "March", "April", "May", "June","July", "August",
                  "September", "October", "November"],
          datasets: [{
            fill: false,
            label: str1,
            lineTension: 0.5,
            borderColor: 'rgba(65,105,225)',
            borderWidth: 2,
            data: temp1
          },
          {
            fill: false,
            label: str2,
            lineTension: 0.5,
            borderColor: 'rgba(220,20,60)',
            borderWidth: 2,
            data: temp2
          },
          {
            fill: false,
            label: str3,
            lineTension: 0.5,
            borderColor: 'rgba(0,0,0,1)',
            borderWidth: 2,
            data: temp3
          }],

          opts: {
            title:{
              display:true,
              text:'Monthly Flights for '.concat(str1).concat(', ').concat(str2).concat(', and ').concat(str3),
              fontSize:20
            },
            animation: {
              duration: 0
            },
            hover: {
              animationDuration: 0
            },
            responsiveAnimationDuration: 0,
            legend: {
              display:true
            }
          }
        });

        info["labels"] = this.state.labels;
        info["datasets"] = this.state.datasets;

        setInterval(() => {}, 10000);

        this.setState({
          updated: true
        });
      }
    });

    this.state.socket.on('Query 5',data => {
      if(!this.state.updated) {
        var l = data.length;
        var c = [];
        var l1 = [];
        var l2 = [];
        var l3 = [];
        var h1 = [];
        var h2 = [];
        var h3 = [];
        var cs = [];
        var l1s = [];
        var l2s = [];
        var l3s = [];
        var h1s = [];
        var h2s = [];
        var h3s = [];
        
        var str = this.state.list[0];

        for(var i = 0; i < l; i++) {
          c.push(data[i]["C"]);
          l1.push(data[i]["L1"]);
          l2.push(data[i]["L2"]);
          l3.push(data[i]["L3"]);
          h1.push(data[i]["H1"]);
          h2.push(data[i]["H2"]);
          h3.push(data[i]["H3"]);
          cs.push(data[i]["CS"]);
          l1s.push(data[i]["L1s"]);
          l2s.push(data[i]["L2S"]);
          l3s.push(data[i]["L3S"]);
          h1s.push(data[i]["H1S"]);
          h2s.push(data[i]["H2S"]);
          h3s.push(data[i]["H3S"]);
        }
        
        console.log(c);

        this.setState({
          labels: ["January", "February", "March", "April", "May", "June","July", "August",
                  "September", "October", "November", "December"],
          datasets: [{
            fill: false,
            label: str,
            lineTension: 0.5,
            borderColor: 'rgb(22,150,210)',
            borderWidth: 2,
            data: cs
          },
          {
            fill: false,
            label: 'Burundi',
            lineTension: 0.5,
            borderColor: 'rgb(210,210,210)',
            borderWidth: 2,
            data: l1s
          },
          {
            fill: false,
            label: 'Nicaragua',
            lineTension: 0.5,
            borderColor: 'rgb(0,0,0)',
            borderWidth: 2,
            data: l2s
          },
          {
            fill: false,
            label: 'Belarus',
            lineTension: 0.5,
            borderColor: 'rgb(253,191,17)',
            borderWidth: 2,
            data: l3s
          },
          {
            fill: false,
            label: 'Libya',
            lineTension: 0.5,
            borderColor: 'rgb(139,0,139)',
            borderWidth: 2,
            data: h1s
          },
          {
            fill: false,
            label: 'Eritrea',
            lineTension: 0.5,
            borderColor: 'rgb(85,183,72)',
            borderWidth: 2,
            data: h2s
          },
          {
            fill: false,
            label: 'Honduras',
            lineTension: 0.5,
            borderColor: 'rgb(219,43,39)',
            borderWidth: 2,
            data: h3s
          }],

          opts: {
            title:{
              display:true,
              text:'Stringency per month in '.concat(str).concat(' and the 3 highest/lowest stringency countries'),
              fontSize:20
            },
            animation: {
              duration: 0
            },
            hover: {
              animationDuration: 0
            },
            responsiveAnimationDuration: 0,
            legend: {
              display:true
            }
          },

          labels2: ["January", "February", "March", "April", "May", "June","July", "August",
          "September", "October", "November", "December"],
          dataset2: [{
            label: str,
            backgroundColor: 'rgba(65,105,225)',
            data: c,
          },
          {
            label: 'Burundi',
            backgroundColor: 'rgba(220,20,60)',
            data: l1,
          },
          {
            label: 'Nicaragua',
            backgroundColor: 'rgba(34,139,34)',
            data: l2,
          },
          {
            label: 'Belarus',
            backgroundColor: 'rgba(0, 0, 1)',
            data: l3,
          }],

          opts2: {
            title:{
              display:true,
              text:'Deaths per million per month in '.concat(str).concat(' and the three least stringent countries'),
              fontSize:20
            },
            animation: {
              duration: 0
            },
            hover: {
              animationDuration: 0
            },
            responsiveAnimationDuration: 0,
            legend: {
              display:true
            }
          },

          labels3: ["January", "February", "March", "April", "May", "June","July", "August",
          "September", "October", "November", "December"],
          dataset3: [{
            label: str,
            backgroundColor: 'rgba(65,105,225)',
            data: c,
          },
          {
            label: 'Libya',
            backgroundColor: 'rgba(220,20,60)',
            data: h1,
          },
          {
            label: 'Eritrea',
            backgroundColor: 'rgba(34,139,34)',
            data: h2,
          },
          {
            label: 'Honduras',
            backgroundColor: 'rgba(0, 0, 1)',
            data: h3,
          }],

          opts3: {
            title:{
              display:true,
              text:'Deaths per million per month in '.concat(str).concat(' and the three most stringent countries'),
              fontSize:20
            },
            animation: {
              duration: 0
            },
            hover: {
              animationDuration: 0
            },
            responsiveAnimationDuration: 0,
            legend: {
              display:true
            }
          },
        });

        console.log(this.state.dataset2);

        info["labels"] = this.state.labels;
        info["datasets"] = this.state.datasets;
        info2["labels"] = this.state.labels2;
        info2["datasets"] = this.state.dataset2;
        info3["labels"] = this.state.labels3;
        info3["datasets"] = this.state.dataset3;

        setInterval(() => {}, 10000);

        this.setState({
          updated: true
        });
      }
    });
  }

  render() {
    let emptyState = (
      <div className="empty">
      </div>
    );

    let loadedState = (
      <div className="loaded">
          <Line
            data={info}
            options={this.state.opts}
          />
      </div>
    );

    let loadedState2 = (
      <div className="loaded">
          <Line
            data={info}
            options={this.state.opts}
          />
          <Line
            data={info2}
            options={this.state.opts2}
          />
      </div>
    );

    let loadedState3Line = (
      <div className="loaded">
          <Line
            data={info}
            options={this.state.opts}
          />
          <Line
            data={info2}
            options={this.state.opts2}
          />
          <Bar
            data={info3}
            options={this.state.opts3}
          />
      </div>
    );

    let loadedState2Bar = (
      <div className="loaded">
          <Line
            data={info}
            options={this.state.opts}
          />
          <Bar
            data={info2}
            options={this.state.opts2}
          />
      </div>
    );

    let loadedState3Bar = (
      <div className="loaded">
          <Line
            data={info}
            options={this.state.opts}
          />
          <Bar
            data={info2}
            options={this.state.opts2}
          />
          <Bar
            data={info3}
            options={this.state.opts3}
          />
      </div>
    );

    return (
      <div className="App">
        <div class="flex-container-header">
          <div class="wrap">
            <h1>Covid-19 Flight Trends</h1>
            <h2>Using SQL to discover new trends between Covid-19 and International Flights</h2>
          </div>
        </div>
        <div class="flex-container-bar">
          <div class="flex-container-helper">
            <div class="dropdown">
              <button class="drop-button">
                <a href="/">Home</a>
              </button>
            </div>
            <div class="dropdown">
              <button class="drop-button">
                <a href="/query1">Query 1</a>
              </button>
            </div>
            <div class="dropdown">
              <button class="drop-button">
                <a href="/query2">Query 2</a>
              </button>
            </div>
            <div class="dropdown">
              <button class="drop-button">
                <a href="/query3">Query 3</a>
              </button>
            </div>
            <div class="dropdown">
              <button class="drop-button">
                <a href="/query4">Query 4</a>
              </button>
            </div>
            <div class="dropdown">
              <button class="drop-button">
                <a href="/query5">Query 5</a>
              </button>
            </div>
          </div>
        </div>
        <BrowserRouter>
          <Switch>
            <Route path='/query1'>
              <div>
                <div class="transbox">
                    <h1>Query 1</h1>
                </div>
                <div class="flex-container-body">
                    <div>
                        <p>
                            Proin sagittis nisl rhoncus mattis rhoncus. Amet venenatis urna cursus eget nunc. Convallis a cras semper auctor neque.
                            Non blandit massa enim nec. Quis ipsum suspendisse ultrices gravida dictum. Lectus quam id leo in vitae. Urna cursus
                            eget nunc scelerisque viverra mauris. Aliquam faucibus purus in massa tempor nec feugiat nisl. Ante metus dictum at
                            tempor commodo ullamcorper a. Fames ac turpis egestas integer eget aliquet nibh praesent. Sagittis eu volutpat odio
                            facilisis mauris.
                        </p>
                    </div>
                    <div className="content">
                      <div className="container">
                        <section className="section">
                          <form className="form" id="addItemForm">
                            <h2>Country</h2>
                            <input
                              type="text"
                              className="input"
                              id="search1"
                              placeholder="Country..."
                              autoComplete="off"
                            />
                            <h2>Country</h2>
                            <input
                              type="text"
                              className="input"
                              id="search2"
                              placeholder="Country..."
                              autoComplete="off"
                            />
                            <h2>Country</h2>
                            <input
                              type="text"
                              className="input"
                              id="search3"
                              placeholder="Country..."
                              autoComplete="off"
                            />
                            <button className="button is-info" onClick={this.addItem1}>
                              Search
                            </button>
                          </form>
                        </section>
                      </div>
                    </div>
                    {!this.state.updated && emptyState}
                    {this.state.updated && loadedState3Line}
                </div>
              </div>
            </Route> 
            <Route path='/query2'>
              <div>
                <div class="transbox">
                    <h1>Query 2</h1>
                </div>
                <div class="flex-container-body">
                    <div>
                        <p>
                            Pulvinar elementum integer enim neque volutpat. Posuere morbi leo urna molestie at elementum eu. Interdum velit euismod
                            in pellentesque. Velit aliquet sagittis id consectetur purus ut faucibus pulvinar. Hac habitasse platea dictumst quisque
                            sagittis purus. Imperdiet massa tincidunt nunc pulvinar sapien et ligula. Malesuada fames ac turpis egestas. Facilisis
                            gravida neque convallis a cras semper. Amet massa vitae tortor condimentum lacinia quis. Duis at consectetur lorem donec
                            massa. Vulputate eu scelerisque felis imperdiet. Mi proin sed libero enim sed faucibus turpis in. Ut aliquam purus sit
                            amet luctus venenatis lectus magna fringilla. Etiam dignissim diam quis enim lobortis. Ac odio tempor orci dapibus
                            ultrices in iaculis nunc sed. Ultrices gravida dictum fusce ut placerat orci nulla.
                        </p>
                    </div>
                    <div className="content">
                      <div className="container">
                        <section className="section">
                          <form className="form" id="addItemForm">
                            <h2>Country</h2>
                            <input
                              type="text"
                              className="input"
                              id="search"
                              placeholder="Country..."
                              autoComplete="off"
                            />
                            <button className="button is-info" onClick={this.addItem2}>
                              Search
                            </button>
                          </form>
                        </section>
                      </div>
                    </div>
                    {!this.state.updated && emptyState}
                    {this.state.updated && loadedState2}
                </div>
              </div>
            </Route>
            <Route path='/query3'>
              <div>
                <div class="transbox">
                    <h1>Query 3</h1>
                </div>
                <div class="flex-container-body">
                    <div>
                        <p>
                            Egestas pretium aenean pharetra magna ac placerat vestibulum. Nullam non nisi est sit amet facilisis magna etiam. Elit duis
                            tristique sollicitudin nibh sit. Diam donec adipiscing tristique risus nec. Convallis posuere morbi leo urna molestie.
                            Suscipit tellus mauris a diam maecenas sed enim. A cras semper auctor neque vitae. Id venenatis a condimentum vitae sapien
                            pellentesque habitant morbi tristique. Commodo nulla facilisi nullam vehicula. Enim facilisis gravida neque convallis a cras
                            semper. Sit amet purus gravida quis blandit. Nullam non nisi est sit amet facilisis magna. Habitant morbi tristique senectus
                            et netus et malesuada. Ut sem viverra aliquet eget sit amet. Tincidunt eget nullam non nisi est. Nisl vel pretium lectus
                            quam id leo in vitae turpis. Sagittis purus sit amet volutpat consequat. Pulvinar pellentesque habitant morbi tristique.
                        </p>
                    </div>
                    <div className="content">
                      <div className="container">
                        <section className="section">
                          <form className="form" id="addItemForm">
                          <h2>Country</h2>
                            <input
                              type="text"
                              className="input"
                              id="search"
                              placeholder="Country..."
                              autoComplete="off"
                            />
                            <button className="button is-info" onClick={this.addItem3}>
                              Search
                            </button>
                          </form>
                        </section>
                      </div>
                    </div>
                    {!this.state.updated && emptyState}
                    {this.state.updated && loadedState2Bar}
                </div>
              </div>
            </Route>
            <Route path='/query4'>
              <div>
                <div class="transbox">
                    <h1>Query 4</h1>
                </div>
                <div class="flex-container-body">
                    <div>
                        <p>
                            Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore
                            magna aliqua. Metus aliquam eleifend mi in nulla. Commodo sed egestas egestas fringilla phasellus faucibus
                            scelerisque eleifend. Tortor pretium viverra suspendisse potenti nullam. Vulputate mi sit amet mauris.
                            Scelerisque eu ultrices vitae auctor eu augue ut lectus. Cursus vitae congue mauris rhoncus aenean. Elementum
                            tempus egestas sed sed risus pretium quam. Et tortor consequat id porta. Egestas fringilla phasellus faucibus
                            scelerisque eleifend. Arcu risus quis varius quam quisque id diam vel quam. Ut eu sem integer vitae justo eget
                            magna. Cursus euismod quis viverra nibh cras pulvinar mattis.
                        </p>
                    </div>
                    <div className="content">
                      <div className="container">
                        <section className="section">
                          <form className="form" id="addItemForm">
                          <h2>Airline 1</h2>
                            <input
                              type="text"
                              className="input"
                              id="search1"
                              placeholder="Airline..."
                              autoComplete="off"
                            />
                            <h2>Airline 2</h2>
                            <input
                              type="text"
                              className="input"
                              id="search2"
                              placeholder="Airline..."
                              autoComplete="off"
                            />
                            <h2>Airline 3</h2>
                            <input
                              type="text"
                              className="input"
                              id="search3"
                              placeholder="Arline..."
                              autoComplete="off"
                            />
                            <button className="button is-info" onClick={this.addItem4}>
                              Search
                            </button>
                          </form>
                        </section>
                      </div>
                    </div>
                    {!this.state.updated && emptyState}
                    {this.state.updated && loadedState}
                </div>
              </div>
            </Route>
            <Route path='/query5'>
              <div>
                <div class="transbox">
                    <h1>Query 5</h1>
                </div>
                <div class="flex-container-body">
                    <div>
                        <p>
                        Consectetur adipiscing elit duis tristique sollicitudin nibh sit amet commodo. Augue ut lectus arcu bibendum at
                            varius. Nibh tortor id aliquet lectus. Semper feugiat nibh sed pulvinar proin. Odio morbi quis commodo odio aenean.
                            Sed viverra ipsum nunc aliquet. Congue nisi vitae suscipit tellus mauris a diam. Convallis aenean et tortor at.
                            Consectetur adipiscing elit pellentesque habitant. Adipiscing elit pellentesque habitant morbi tristique senectus et
                            netus. Elementum curabitur vitae nunc sed velit. Enim blandit volutpat maecenas volutpat blandit aliquam.
                        </p>
                    </div>
                    <div className="content">
                      <div className="container">
                        <section className="section">
                          <form className="form" id="addItemForm">
                            <h2>Country</h2>
                            <input
                              type="text"
                              className="input"
                              id="search"
                              placeholder="Country..."
                              autoComplete="off"
                            />
                            <button className="button is-info" onClick={this.addItem5}>
                              Search
                            </button>
                          </form>
                        </section>
                      </div>
                    </div>
                    {!this.state.updated && emptyState}
                    {this.state.updated && loadedState3Bar}
                </div>
              </div>
            </Route> 
            <Route path='/'>
              <div>
                <div class="transbox">
                    <h1>About Us</h1>
                </div>
                <div class="flex-container-body">
                    <div>
                        <p>
                            Proin sagittis nisl rhoncus mattis rhoncus. Amet venenatis urna cursus eget nunc. Convallis a cras semper auctor neque.
                            Non blandit massa enim nec. Quis ipsum suspendisse ultrices gravida dictum. Lectus quam id leo in vitae. Urna cursus
                            eget nunc scelerisque viverra mauris. Aliquam faucibus purus in massa tempor nec feugiat nisl. Ante metus dictum at
                            tempor commodo ullamcorper a. Fames ac turpis egestas integer eget aliquet nibh praesent. Sagittis eu volutpat odio
                            facilisis mauris.
                        </p>
                        <p>
                            Pulvinar elementum integer enim neque volutpat. Posuere morbi leo urna molestie at elementum eu. Interdum velit euismod
                            in pellentesque. Velit aliquet sagittis id consectetur purus ut faucibus pulvinar. Hac habitasse platea dictumst quisque
                            sagittis purus. Imperdiet massa tincidunt nunc pulvinar sapien et ligula. Malesuada fames ac turpis egestas. Facilisis
                            gravida neque convallis a cras semper. Amet massa vitae tortor condimentum lacinia quis. Duis at consectetur lorem donec
                            massa. Vulputate eu scelerisque felis imperdiet. Mi proin sed libero enim sed faucibus turpis in. Ut aliquam purus sit
                            amet luctus venenatis lectus magna fringilla. Etiam dignissim diam quis enim lobortis. Ac odio tempor orci dapibus
                            ultrices in iaculis nunc sed. Ultrices gravida dictum fusce ut placerat orci nulla.
                        </p>
                        <p>
                            Netus et malesuada fames ac turpis egestas. Eu mi bibendum neque egestas congue quisque egestas. Facilisis sed odio morbi
                            quis commodo odio. Eget nunc lobortis mattis aliquam faucibus purus. Donec enim diam vulputate ut pharetra sit. Pharetra
                            pharetra massa massa ultricies mi quis hendrerit dolor magna. Sed tempus urna et pharetra pharetra. Facilisis volutpat
                            est velit egestas dui id ornare arcu. Ornare massa eget egestas purus viverra accumsan in nisl nisi. Leo integer malesuada
                            nunc vel risus commodo. Sagittis nisl rhoncus mattis rhoncus urna neque viverra. Rhoncus mattis rhoncus urna neque viverra.
                            Tempus imperdiet nulla malesuada pellentesque elit eget gravida cum.
                        </p>
                        <p>
                            Egestas pretium aenean pharetra magna ac placerat vestibulum. Nullam non nisi est sit amet facilisis magna etiam. Elit duis
                            tristique sollicitudin nibh sit. Diam donec adipiscing tristique risus nec. Convallis posuere morbi leo urna molestie.
                            Suscipit tellus mauris a diam maecenas sed enim. A cras semper auctor neque vitae. Id venenatis a condimentum vitae sapien
                            pellentesque habitant morbi tristique. Commodo nulla facilisi nullam vehicula. Enim facilisis gravida neque convallis a cras
                            semper. Sit amet purus gravida quis blandit. Nullam non nisi est sit amet facilisis magna. Habitant morbi tristique senectus
                            et netus et malesuada. Ut sem viverra aliquet eget sit amet. Tincidunt eget nullam non nisi est. Nisl vel pretium lectus
                            quam id leo in vitae turpis. Sagittis purus sit amet volutpat consequat. Pulvinar pellentesque habitant morbi tristique.
                        </p>
                    </div>
                </div>
              </div>
            </Route>
          </Switch>
        </BrowserRouter>
      </div>
    );
  }
}

export default App;
