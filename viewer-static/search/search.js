var entityMap = {
'&': '&amp;',
'<': '&lt;',
'>': '&gt;',
'"': '&quot;',
"'": '&#39;',
'/': '&#x2F;',
'`': '&#x60;',
'=': '&#x3D;'
};

function escapeHtml (string) {
return String(string).replace(/[&<>"'`=\/]/g, function (s) {
return entityMap[s];
});
}

async function doAPIcall(query) {
    let url = `/api/unstable/search?${query}`;
    try {
        let res = await fetch(url);
        return await res.json();
    } catch (error) {
        console.log(error);
    }
}

async function renderResults(urlParams) {
    let html = '';
    let response = await doAPIcall(urlParams);
    response.results.forEach(res => {

        let htmlSegment = `<br /><div class="result">
                           <h3>${res.subject}</h3><p>${escapeHtml(res.text)}</p>
                           <a href="${res.url}">${res.url}</a><span style="float: right;">${res.created}</span></div>`;

        html += htmlSegment;
    });

    let container = document.querySelector('.results-container');
    container.innerHTML = html;
}

let params = new URLSearchParams(window.location.search);
const searchQuery = params.get('q');

let keysForDel = [];
params.forEach((value, key) => {
  if (value == '') {
    keysForDel.push(key);
  }
});

keysForDel.forEach(key => {
  params.delete(key);
});
console.log(params);
document.getElementById("results-searchbox").value = escapeHtml(searchQuery);
renderResults(params);
