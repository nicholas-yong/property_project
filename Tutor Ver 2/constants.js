let currencyFormatter = new Intl.NumberFormat( 'en-au',
{
    style: 'currency',
    currency: 'AUD',
    currencyDisplay: "narrowSymbol",
    minimumFractionDigits: 2 
})

exports.currencyFormatter = currencyFormatter