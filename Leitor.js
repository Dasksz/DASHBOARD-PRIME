/**
 * parser.worker.js
 * * This worker handles all the heavy data processing tasks, including:
 * - Reading uploaded files (CSV, XLSX).
 * - Parsing and cleaning data (dates, numbers).
 * - Applying business logic (merging data, handling special cases).
 * - Aggregating data.
 * * This ensures the main UI thread remains responsive and never freezes,
 * providing a smooth user experience.
 */

// Import the SheetJS library (XLSX) for parsing Excel files.
self.importScripts('https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js');

/**
 * Parses a date string from various possible formats into a JavaScript Date object.
 * Handles Excel's numeric date format, Brazilian DD/MM/YYYY, and standard ISO formats.
 * @param {string|number|Date} dateString - The date value to parse.
 * @returns {Date|null} A Date object or null if parsing fails.
 */
function parseDate(dateString) {
    if (!dateString) return null;
    if (dateString instanceof Date) return !isNaN(dateString.getTime()) ? dateString : null;
    // Handle Excel's numeric date format (days since 1900).
    if (typeof dateString === 'number') return new Date(Math.round((dateString - 25569) * 86400 * 1000));
    if (typeof dateString !== 'string') return null;
    
    // Handle Brazilian date format (DD/MM/YYYY).
    const parts = dateString.split('/');
    if (parts.length === 3) {
        const [day, month, year] = parts;
        if (day.length === 2 && month.length === 2 && year.length === 4) {
            // Construct in YYYY-MM-DD format to avoid timezone issues.
            return new Date(`${year}-${month}-${day}T00:00:00`);
        }
    }
    
    // Fallback to standard ISO date parsing.
    const isoDate = new Date(dateString);
    return !isNaN(isoDate.getTime()) ? isoDate : null;
}

/**
 * Reads the content of a file (CSV or XLSX) and converts it to a JSON array.
 * @param {File} file - The file object to read.
 * @returns {Promise<Array<Object>>} A promise that resolves with the parsed JSON data.
 */
const readFile = (file) => {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = (event) => {
            try {
                let jsonData;
                const data = event.target.result;
                if (file.name.endsWith('.csv')) {
                    // Decode using iso-8859-1 for compatibility with Brazilian systems.
                    const decodedData = new TextDecoder('iso-8859-1').decode(new Uint8Array(data));
                    const lines = decodedData.split(/\r?\n/).filter(line => line.trim() !== '');
                    if (lines.length < 1) throw new Error(`Arquivo CSV '${file.name}' inválido ou vazio.`);
                    
                    // Auto-detect delimiter.
                    const firstLine = lines[0];
                    const delimiter = firstLine.includes(';') ? ';' : ',';

                    const headers = lines.shift().trim().split(delimiter);
                    jsonData = lines.map(line => {
                        const values = line.trim().split(delimiter);
                        let row = {};
                        headers.forEach((header, index) => {
                            row[header.trim()] = values[index] || null;
                        });
                        return row;
                    });
                } else {
                    // Use XLSX library for Excel files.
                    const workbook = XLSX.read(new Uint8Array(data), {type: 'array'});
                    const firstSheetName = workbook.SheetNames[0];
                    const worksheet = workbook.Sheets[firstSheetName];
                    jsonData = XLSX.utils.sheet_to_json(worksheet, { raw: false, cellDates: true });
                }
                resolve(jsonData);
            } catch (error) {
                reject(error);
            }
        };
        reader.onerror = () => reject(new Error(`Erro ao ler o arquivo '${file.name}'.`));
        reader.readAsArrayBuffer(file);
    });
};

/**
 * Parses a number string in Brazilian format (e.g., "1.234,56") into a float.
 * @param {string|number} value - The value to parse.
 * @returns {number} The parsed number, or 0 if invalid.
 */
function parseBrazilianNumber(value) {
    if (typeof value === 'number') return value;
    if (typeof value !== 'string' || !value) return 0;
    // Clean up the string, removing currency symbols and whitespace.
    const cleaned = String(value).replace(/R\$\s?/g, '').trim();
    const lastComma = cleaned.lastIndexOf(',');
    const lastDot = cleaned.lastIndexOf('.');
    let numberString;
    // Determine which character is the decimal separator.
    if (lastComma > lastDot) {
        // Comma is decimal, dots are thousands separators.
        numberString = cleaned.replace(/\./g, '').replace(',', '.');
    } else if (lastDot > lastComma) {
        // Dot is decimal, commas are thousands separators (less common in BR).
        numberString = cleaned.replace(/,/g, '');
    } else {
        // No thousands separator, or ambiguous. Assume comma is decimal.
        numberString = cleaned.replace(',', '.');
    }
    const number = parseFloat(numberString);
    return isNaN(number) ? 0 : number;
}

/**
 * Processes raw sales data, applying business logic and mapping related data.
 * @param {Array<Object>} rawData - The raw sales data array.
 * @param {Map} clientMap - A map of client data, keyed by client ID.
 * @param {Map} productMasterMap - A map of product master package quantities.
 * @returns {Array<Object>} The processed and enriched sales data.
 */
const processSalesData = (rawData, clientMap, productMasterMap) => {
    return rawData.map(rawRow => {
        const clientInfo = clientMap.get(String(rawRow['CODCLI'])) || {};
        let vendorName = String(rawRow['NOME'] || '');
        let supervisorName = String(rawRow['SUPERV'] || '');
        let codUsur = String(rawRow['CODUSUR'] || '');
        const pedido = String(rawRow['PEDIDO'] || '');

        // --- Business Logic Rules ---
        if (supervisorName.trim().toUpperCase() === 'OSÉAS SANTOS OL') supervisorName = 'OSVALDO NUNES O';
        const nomeClienteParaLogica = (clientInfo.razaoSocial || '').toUpperCase();
        const supervisorUpper = (supervisorName || '').trim().toUpperCase();
        if (supervisorUpper === 'BALCAO' || supervisorUpper === 'BALCÃO') supervisorName = 'BALCAO';
        if (supervisorName === 'BALCAO' && nomeClienteParaLogica.includes('AMERICANAS')) { vendorName = 'AMERICANAS'; codUsur = '1001'; }
        if (pedido.startsWith('120')) { vendorName = 'VD HIAGO'; supervisorName = 'HIAGO ASSUNCAO'; codUsur = '1002'; }
        
        let dtPed = rawRow['DTPED'];
        const dtSaida = rawRow['DTSAIDA'];

        // Logic to correct order date if it's in a different month/year from the exit date.
        const parsedDtPed = parseDate(dtPed);
        const parsedDtSaida = parseDate(dtSaida);
        if (parsedDtPed && parsedDtSaida && (parsedDtPed.getFullYear() < parsedDtSaida.getFullYear() || (parsedDtPed.getFullYear() === parsedDtSaida.getFullYear() && parsedDtPed.getMonth() < parsedDtSaida.getMonth()))) {
            dtPed = dtSaida;
        }
        
        const productCode = String(rawRow['PRODUTO'] || '');
        const qtdeMaster = productMasterMap.get(productCode) || 1;
        const qtVenda = parseInt(String(rawRow['QTVENDA'] || '0').trim(), 10);

        return {
            PEDIDO: pedido, NOME: vendorName, SUPERV: supervisorName, PRODUTO: productCode,
            DESCRICAO: String(rawRow['DESCRICAO'] || ''), FORNECEDOR: String(rawRow['FORNECEDOR'] || ''),
            OBSERVACAOFOR: String(rawRow['OBSERVACAOFOR'] || '').trim(), CODFOR: String(rawRow['CODFOR'] || ''),
            CODUSUR: codUsur, CODCLI: String(rawRow['CODCLI'] || ''), CLIENTE_NOME: clientInfo.nomeCliente || 'N/A',
            CIDADE: clientInfo.cidade || 'N/A', BAIRRO: clientInfo.bairro || 'N/A',
            QTVENDA: qtVenda, VLVENDA: parseBrazilianNumber(rawRow['VLVENDA']),
            TOTPESOLIQ: parseBrazilianNumber(rawRow['TOTPESOLIQ']), DTPED: dtPed, DTSAIDA: dtSaida, 
            POSICAO: String(rawRow['POSICAO'] || ''),
            QTVENDA_EMBALAGEM_MASTER: qtVenda / qtdeMaster
        };
    });
};

/**
 * Main event listener for the worker. Triggered when the UI sends a message.
 * This function orchestrates the entire data processing pipeline.
 */
self.onmessage = async (event) => {
    const { salesFile, clientsFile, productsFile, historyFile } = event.data;

    try {
        // Send progress updates back to the UI.
        self.postMessage({ type: 'progress', status: 'Lendo arquivos...', percentage: 10 });
        const [salesDataRaw, clientsDataRaw, productsDataRaw, historyDataRaw] = await Promise.all([
            readFile(salesFile),
            readFile(clientsFile),
            readFile(productsFile),
            readFile(historyFile)
        ]);

        self.postMessage({ type: 'progress', status: 'Mapeando produtos...', percentage: 30 });
        const productMasterMap = new Map();
        productsDataRaw.forEach(prod => {
            const productCode = String(prod['Código'] || '');
            if (!productCode) return;
            let qtdeMaster = parseInt(prod['Qtde embalagem master(Compra)'], 10);
            if (isNaN(qtdeMaster) || qtdeMaster <= 0) qtdeMaster = 1;
            productMasterMap.set(productCode, qtdeMaster);
        });

        const clientRcaOverrides = new Map();
        salesDataRaw.forEach(rawRow => {
            const pedido = String(rawRow['PEDIDO'] || '');
            const codCli = String(rawRow['CODCLI'] || '');
            if(!codCli) return;
            if (pedido.startsWith('120')) clientRcaOverrides.set(codCli, '1002');
        });

        self.postMessage({ type: 'progress', status: 'Processando clientes...', percentage: 50 });
        const clientMap = new Map();
        clientsDataRaw.forEach(client => {
            const codCli = String(client['Código'] || '');
            if (!codCli) return;
            const rcas = new Set();
            if (client['RCA 1']) rcas.add(String(client['RCA 1']));
            if (client['RCA 2']) rcas.add(String(client['RCA 2']));
            let registrationDateStr = client['Data e Hora de Cadastro'] || '';
            if (registrationDateStr.includes(' ')) registrationDateStr = registrationDateStr.split(' ')[0];
            const clientData = {
                'Código': codCli, rcas: Array.from(rcas), cidade: String(client['Nome da Cidade'] || 'N/A'),
                nomeCliente: String(client['Fantasia'] || client['Cliente'] || 'N/A'), bairro: String(client['Bairro'] || 'N/A'),
                razaoSocial: String(client['Cliente'] || 'N/A'), fantasia: String(client['Fantasia'] || 'N/A'),
                cnpj_cpf: String(client['CNPJ/CPF'] || 'N/A'), endereco: String(client['Endereço Comercial'] || client['Endereço'] || 'N/A'),
                numero: String(client['Número'] || 'SN'), cep: String(client['CEP'] || 'N/A'), telefone: String(client['Telefone Comercial'] || 'N/A'),
                email: String(client['E-mail'] || 'N/A'), ramo: String(client['Ramo Atividade'] || 'N/A'),
                ultimaCompra: client['Data da Última Compra'], dataCadastro: registrationDateStr,
                bloqueio: String(client['Bloqueio'] || '').trim().toUpperCase(), inscricaoEstadual: String(client['Insc. Est. / Produtor'] || 'N/A')
            };
            if (clientRcaOverrides.has(codCli)) clientData.rcas.unshift(clientRcaOverrides.get(codCli));
            if (clientData.razaoSocial.toUpperCase().includes('AMERICANAS')) clientData.rcas.unshift('1001');
            clientMap.set(codCli, clientData);
        });

        self.postMessage({ type: 'progress', status: 'Cruzando dados de vendas...', percentage: 70 });
        const processedSalesData = processSalesData(salesDataRaw, clientMap, productMasterMap);
        const processedHistoryData = processSalesData(historyDataRaw, clientMap, productMasterMap);

        self.postMessage({ type: 'progress', status: 'Atualizando datas de compra...', percentage: 80 });
        const latestSaleDateByClient = new Map();
        processedSalesData.forEach(sale => {
            const codcli = sale.CODCLI;
            const saleDate = parseDate(sale.DTPED);
            if (codcli && saleDate) {
                const existingDate = latestSaleDateByClient.get(codcli);
                if (!existingDate || saleDate > existingDate) latestSaleDateByClient.set(codcli, saleDate);
            }
        });
        clientMap.forEach((client, codcli) => {
            const lastPurchaseDate = parseDate(client.ultimaCompra);
            const latestSaleDate = latestSaleDateByClient.get(codcli);
            if (latestSaleDate && (!lastPurchaseDate || isNaN(lastPurchaseDate.getTime()) || latestSaleDate > latestSaleDate)) {
                client.ultimaCompra = latestSaleDate;
            }
        });
        
        self.postMessage({ type: 'progress', status: 'Agregando pedidos...', percentage: 90 });
        const aggregateOrders = (data) => {
            const orders = {};
            data.forEach(row => {
                if (!row.PEDIDO) return;
                if (!orders[row.PEDIDO]) {
                    orders[row.PEDIDO] = { ...row, QTVENDA: 0, VLVENDA: 0, TOTPESOLIQ: 0, FORNECEDORES: new Set() };
                }
                orders[row.PEDIDO].QTVENDA += row.QTVENDA;
                orders[row.PEDIDO].VLVENDA += row.VLVENDA;
                orders[row.PEDIDO].TOTPESOLIQ += row.TOTPESOLIQ;
                if (row.OBSERVACAOFOR) orders[row.PEDIDO].FORNECEDORES.add(row.OBSERVACAOFOR);
            });
            return Object.values(orders).map(order => {
                order.FORNECEDORES_LIST = Array.from(order.FORNECEDORES);
                order.FORNECEDORES_STR = order.FORNECEDORES_LIST.join(', ');
                return order;
            });
        };
        const aggregatedByOrder = aggregateOrders(processedSalesData);
        
        self.postMessage({ type: 'progress', status: 'Finalizando...', percentage: 100 });
        
        // Send the final result back to the main page.
        self.postMessage({
            type: 'result',
            data: {
                detailed: processedSalesData,
                history: processedHistoryData,
                byOrder: aggregatedByOrder,
                clients: Array.from(clientMap.values())
            }
        });

    } catch (error) {
        // In case of any error during the process, send an error message.
        self.postMessage({ type: 'error', message: error.message });
    }
};
