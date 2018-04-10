Highcharts.chart('London container', {

  title: {
    text: ' '
  },

  series: [{
    keys: ['from', 'to', 'weight'],
    data: [
      ['London, UK', 'London', 408],
      ['New York, USA', 'London', 34],
      ['Exeter, UK', 'London', 32],
      ['San Francisco, USA', 'London', 32],
      ['Palo Alto, USA', 'London', 14],
      ['Mountain View, USA', 'London', 13],
      ['Paris, France', 'London', 12]
    ],
    type: 'sankey',
    name: 'Number of Investors'
  }]

});

Highcharts.chart('UK container', {

  title: {
    text: ' '
  },

  series: [{
    keys: ['from', 'to', 'weight'],
    data: [
        ['United Kingdom', 'London', 486],
        ['United States', 'London', 135],
        ['United Kingdom', 'Edinburgh', 26],
        ['United Kingdom', 'Bristol', 17],
        ['United Kingdom', 'Cambridge', 14],
        ['Switzerland', 'London', 14],
        ['United Kingdom', 'Manchester', 14],
        ['France', 'London', 12],
        ['United Kingdom', 'Oxford', 12],
        ['Spain', 'London', 11],
        ['United Kingdom', 'Belfast', 10],
        ['United Kingdom', 'Glasgow', 9],
        ['Germany', 'London', 9],
        ['Italy', 'London', 8],
        ['United States', 'Bristol', 8]
    ],
    type: 'sankey',
    name: 'Number of Investors'
  }]

});


(function(document) {
	'use strict';

	var LightTableFilter = (function(Arr) {

		var _input;

		function _onInputEvent(e) {
			_input = e.target;
			var tables = document.getElementsByClassName(_input.getAttribute('data-table'));
			Arr.forEach.call(tables, function(table) {
				Arr.forEach.call(table.tBodies, function(tbody) {
					Arr.forEach.call(tbody.rows, _filter);
				});
			});
		}

		function _filter(row) {
			var text = row.textContent.toLowerCase(), val = _input.value.toLowerCase();
			row.style.display = text.indexOf(val) === -1 ? 'none' : 'table-row';
		}

		return {
			init: function() {
				var inputs = document.getElementsByClassName('light-table-filter');
				Arr.forEach.call(inputs, function(input) {
					input.oninput = _onInputEvent;
				});
			}
		};
	})(Array.prototype);

	document.addEventListener('readystatechange', function() {
		if (document.readyState === 'complete') {
			LightTableFilter.init();
		}
	});

})(document);

