document.addEventListener('DOMContentLoaded', function () {
    var chartDom = document.getElementById('chart');
    var myChart = echarts.init(chartDom);
    
    // Fonction pour charger les données en fonction de la crypto sélectionnée
    function loadCryptoData(crypto) {
        fetch(`/data/${crypto}`)
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    console.error(data.error);
                    return;
                }

                // Extraire les timestamps et les prix de clôture
                var timestamps = data.map(item => new Date(item.Timestamp * 1000).toLocaleString());
                var closePrices = data.map(item => parseFloat(item.Close));

                // Mettre à jour le graphique
                var option = {
                    title: {
                        text: `${crypto} Price Over Time`
                    },
                    xAxis: {
                        type: 'category',
                        data: timestamps,
                        axisLabel: {
                            rotate: 45
                        }
                    },
                    yAxis: {
                        type: 'value',
                        name: 'Price (USD)'
                    },
                    series: [{
                        data: closePrices,
                        type: 'line',
                        smooth: true,
                        areaStyle: {}
                    }]
                };

                myChart.setOption(option);
            })
            .catch(error => console.error('Error fetching data:', error));
    }

    // Charger les données de Bitcoin au chargement initial
    loadCryptoData('BTC');

    // Mettre à jour les données lorsqu'une nouvelle crypto est sélectionnée
    document.getElementById('crypto-select').addEventListener('change', function () {
        var selectedCrypto = this.value;
        loadCryptoData(selectedCrypto);
    });
});
