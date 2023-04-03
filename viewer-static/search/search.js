var entityMap = {
'&': '&amp;',
'<': '&lt;',
'>': '&gt;',
'/': '&#x2F;',
'`': '&#x60;',
'=': '&#x3D;'
};

const timeout = (prom, time, exception) => {
	let timer;
	return Promise.race([
		prom,
		new Promise((_r, rej) => timer = setTimeout(rej, time, exception))
	]).finally(() => clearTimeout(timer));
}

function escapeHtml (string) {
return String(string).replace(/[&<>`=\/]/g, function (s) {
return entityMap[s];
});
}

async function doAPIcall(query) {
    let url = `/api/unstable/search?${query}`;
    return await fetch(url);
}

async function renderResults(urlParams) {
    let statsbanner = document.getElementById("result-stats")

    const timeoutError = Symbol();
    try {
        var res = await timeout(doAPIcall(urlParams), 5000, timeoutError);
        let callok = res.ok;
        let callstatus = res.status;
        res = await res.json();
        if (!callok) {
            throw new Error("HTTP Error " + callstatus);
        }


    }catch (e) {
        if (e === timeoutError) {
            // handle timeout
            statsbanner.innerHTML = "Search timed out. Please try again later.";
            return
        }else {
            // other error
            statsbanner.innerHTML = `Search failed for reason: `;
            if ("detail" in res) {
                for (i of res.detail) {
                    statsbanner.innerHTML += `<br /><br />Field "${i.loc[1]}", ${i.msg}`;
                }
            }else {
                statsbanner.innerHTML += `${e}`};
            throw e;
        }
    }

    //let response = await doAPIcall(urlParams);
    let html = '';
    let query_exp = '<b>must have</b> ';
    res.debug.query.and_terms.forEach(term => {query_exp += `${term}, `})
    res.debug.query.and_phrases.forEach(term => {query_exp += `${term}, `})
    if (res.debug.query.not_terms.length != 0) {
        query_exp += `<b>and must not have</b> `
        res.debug.query.not_terms.forEach(term => {query_exp += `${term}, `})
    }

    document.getElementById("result-stats").innerHTML = `Search for ${query_exp} finished in ${res.debug.dbtime_ms}ms`;
    res.results.forEach(i => {

        html += `<br /><div class="result">
                           <h3>${i.subject}</h3><p>${escapeHtml(i.text)}</p>
                           <a href="${res.url}">${i.url}</a><span style="float: right;">${i.created}</span></div>`;
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
