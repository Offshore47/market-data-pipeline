import React, { useState, useEffect, useRef, useCallback } from 'react';
import { initializeApp } from 'firebase/app';
import { getAuth, signInAnonymously, signInWithCustomToken, onAuthStateChanged } from 'firebase/auth';
import { getFirestore, collection, onSnapshot, query, doc, setDoc } from 'firebase/firestore';
import Chart from 'chart.js/auto';
import { Minus, X, LogIn, TrendingUp, DollarSign } from 'lucide-react';

// --- CONFIGURATION & UTILITIES ---

// NOTE: These variables are provided by the canvas environment.
const appId = typeof __app_id !== 'undefined' ? __app_id : 'default-app-id';
const firebaseConfig = typeof __firebase_config !== 'undefined' ? JSON.parse(__firebase_config) : {};
const initialAuthToken = typeof __initial_auth_token !== 'undefined' ? __initial_auth_token : '';

// IMPORTANT: This key is read from your GitHub setup (CHART_API_KEY secret)
// *** REPLACE THIS WITH YOUR ACTUAL ALPHA VANTAGE/CHARTING API KEY ***
const CHART_API_KEY = "YOUR_CHART_API_KEY_HERE"; 
// Using Alpha Vantage as the chart data source
const FINANCIAL_API_BASE_URL = "https://www.alphavantage.co/query"; 

// --- APP COMPONENT ---

const App = () => {
    const [db, setDb] = useState(null);
    const [auth, setAuth] = useState(null);
    const [userId, setUserId] = useState(null);
    const [isAuthReady, setIsAuthReady] = useState(false);
    const [topStocks, setTopStocks] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [chartData, setChartData] = useState(null);
    const [modalPos, setModalPos] = useState({ x: 50, y: 50 });
    const modalRef = useRef(null);

    // 1. Firebase Initialization and Authentication
    useEffect(() => {
        try {
            const app = initializeApp(firebaseConfig);
            const firestore = getFirestore(app);
            const firebaseAuth = getAuth(app);
            
            // Log in using custom token or anonymously
            const signIn = async () => {
                try {
                    if (initialAuthToken) {
                        await signInWithCustomToken(firebaseAuth, initialAuthToken);
                    } else {
                        await signInAnonymously(firebaseAuth);
                    }
                } catch (e) {
                    console.error("Firebase sign-in failed:", e);
                    setError("Authentication failed. Check console for details.");
                }
            };
            
            signIn().then(() => {
                // Set up Auth State Listener
                onAuthStateChanged(firebaseAuth, (user) => {
                    setAuth(firebaseAuth);
                    setDb(firestore);
                    setUserId(user ? user.uid : null);
                    setIsAuthReady(true);
                    setLoading(false);
                });
            });

        } catch (e) {
            console.error("Firebase initialization failed:", e);
            setError("Failed to initialize Firebase. Check console.");
            setLoading(false);
        }
    }, []);

    // 2. Real-time Firestore Listener for Top Stocks
    useEffect(() => {
        if (!isAuthReady || !db) return;

        // Path for public data: /artifacts/{appId}/public/data/topStocks
        // Uses the default-app-id we set in the GitHub Variable
        const collectionPath = `artifacts/${appId}/public/data/topStocks`;
        const q = query(collection(db, collectionPath));
        
        const unsubscribe = onSnapshot(q, (snapshot) => {
            const stocks = [];
            snapshot.forEach((doc) => {
                stocks.push({ id: doc.id, ...doc.data() });
            });
            // Sort by 'score' as calculated by the Python script
            stocks.sort((a, b) => (b.score || 0) - (a.score || 0));
            setTopStocks(stocks);
            if (loading) setLoading(false);
        }, (err) => {
            console.error("Firestore data snapshot error:", err);
            setError("Could not load stock data.");
            setLoading(false);
        });

        return () => unsubscribe();
    }, [isAuthReady, db]);

    // 3. Data Fetching Function for Charting (Exponential Backoff included)
    const fetchStockData = useCallback(async (symbol) => {
        if (!CHART_API_KEY || CHART_API_KEY === "YOUR_CHART_API_KEY_HERE") {
             setError("Please set a valid CHART_API_KEY to fetch chart data.");
             return;
        }

        const url = `${FINANCIAL_API_BASE_URL}?function=TIME_SERIES_DAILY_ADJUSTED&symbol=${symbol}&apikey=${CHART_API_KEY}`;
        const MAX_RETRIES = 5;
        let attempt = 0;
        
        setChartData({ symbol, data: [], loading: true }); // Show loading state

        while (attempt < MAX_RETRIES) {
            try {
                const response = await fetch(url);
                if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
                
                const data = await response.json();
                
                if (data["Error Message"] || data["Note"]) {
                    throw new Error(data["Error Message"] || data["Note"] || "API returned an error or limit warning. Check your API key and rate limits.");
                }
                
                const timeSeries = data["Time Series (Daily)"];
                if (!timeSeries) {
                    throw new Error("Invalid data structure received from API. Check if the symbol is correct.");
                }

                // Transform data for Chart.js
                const dates = Object.keys(timeSeries).sort();
                const chartPoints = dates.map(date => ({
                    x: date,
                    y: parseFloat(timeSeries[date]["4. close"])
                }));

                setChartData({ symbol, data: chartPoints, loading: false });
                return; // Success
                
            } catch (e) {
                console.warn(`Attempt ${attempt + 1} failed for ${symbol}: ${e.message}`);
                setChartData({ symbol, data: [], loading: false, error: e.message });
                attempt++;
                if (attempt >= MAX_RETRIES) {
                    setError(`Failed to fetch historical data for ${symbol} after ${MAX_RETRIES} attempts. Error: ${e.message}`);
                    break;
                }
                // Exponential backoff delay
                const delay = Math.pow(2, attempt) * 1000 + Math.random() * 1000;
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
    }, []);

    // 4. Movable Chart Modal Logic
    const ChartModal = ({ data, onClose }) => {
        const canvasRef = useRef(null);
        const [isDragging, setIsDragging] = useState(false);
        const dragStartPos = useRef({ x: 0, y: 0 });
        const chartInstance = useRef(null);

        // Chart Rendering Logic
        useEffect(() => {
            if (canvasRef.current && data && data.data.length > 0) {
                if (chartInstance.current) {
                    chartInstance.current.destroy(); 
                }

                const ctx = canvasRef.current.getContext('2d');
                const moment = window.moment; 
                
                chartInstance.current = new Chart(ctx, {
                    type: 'line',
                    data: {
                        datasets: [{
                            label: `${data.symbol} Daily Close Price`,
                            data: data.data,
                            parsing: { xAxisKey: 'x', yAxisKey: 'y' },
                            borderColor: 'rgb(75, 192, 192)',
                            tension: 0.1,
                            pointRadius: 0
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: { display: true },
                            title: { display: true, text: `${data.symbol} Historical Performance`, font: { size: 16 } }
                        },
                        scales: {
                            x: {
                                type: 'time',
                                time: { unit: 'day', tooltipFormat: 'MMM D, YYYY' },
                                title: { display: true, text: 'Date' }
                            },
                            y: { 
                                title: { display: true, text: 'Price ($)' },
                                ticks: {
                                    callback: function(value, index, ticks) {
                                        return '$' + value;
                                    }
                                }
                            }
                        }
                    }
                });
            }
            return () => {
                if (chartInstance.current) {
                    chartInstance.current.destroy();
                }
            };
        }, [data]);


        // Drag Handler Logic
        const handleMouseDown = (e) => {
            if (e.target.closest('.drag-handle')) {
                setIsDragging(true);
                dragStartPos.current = {
                    x: e.clientX - modalPos.x,
                    y: e.clientY - modalPos.y,
                };
            }
        };

        const handleMouseMove = useCallback((e) => {
            if (!isDragging) return;
            let newX = e.clientX - dragStartPos.current.x;
            let newY = e.clientY - dragStartPos.current.y; 
            
            // Keep modal within viewport bounds
            const bounds = modalRef.current.getBoundingClientRect();
            const maxX = window.innerWidth - bounds.width;
            const maxY = window.innerHeight - bounds.height;
            
            newX = Math.max(0, Math.min(newX, maxX));
            newY = Math.max(0, Math.min(newY, maxY));

            setModalPos({ x: newX, y: newY });
        }, [isDragging]);

        const handleMouseUp = useCallback(() => {
            setIsDragging(false);
        }, []);

        useEffect(() => {
            if (isDragging) {
                window.addEventListener('mousemove', handleMouseMove);
                window.addEventListener('mouseup', handleMouseUp);
            } else {
                window.removeEventListener('mousemove', handleMouseMove);
                window.removeEventListener('mouseup', handleMouseUp);
            }
            return () => {
                window.removeEventListener('mousemove', handleMouseMove);
                window.removeEventListener('mouseup', handleMouseUp);
            };
        }, [isDragging, handleMouseMove, handleMouseUp]);


        return (
            <div 
                ref={modalRef}
                className="fixed bg-white/95 backdrop-blur-sm shadow-2xl rounded-xl z-50 border border-gray-200 w-11/12 md:w-3/4 lg:w-2/3 max-w-4xl transition-shadow"
                style={{ top: `${modalPos.y}px`, left: `${modalPos.x}px`, cursor: isDragging ? 'grabbing' : 'default' }}
            >
                <div 
                    className="drag-handle p-3 bg-indigo-600 text-white flex justify-between items-center rounded-t-xl cursor-grab active:cursor-grabbing"
                    onMouseDown={handleMouseDown}
                >
                    <h3 className="text-lg font-bold">Chart: {data.symbol}</h3>
                    <button 
                        onClick={onClose} 
                        className="p-1 rounded-full hover:bg-indigo-700 transition-colors"
                        title="Close Chart"
                    >
                        <X size={20} />
                    </button>
                </div>
                <div className="p-4 h-96 flex items-center justify-center">
                    {data.loading && (
                         <div className="text-indigo-600 animate-spin mr-3">
                            <TrendingUp size={32} />
                         </div>
                    )}
                    {data.error && (
                        <div className="text-red-500 text-center p-4">
                            <p className="font-bold">Data Fetch Error</p>
                            <p className="text-sm">{data.error}</p>
                            <p className="text-xs mt-2">Check console for details or verify your API Key.</p>
                        </div>
                    )}
                    {data.data.length > 0 && !data.loading && (
                        <canvas ref={canvasRef} className="w-full h-full"></canvas>
                    )}
                    {data.data.length === 0 && !data.loading && !data.error && (
                         <div className="text-gray-500 text-center">
                            <p className="font-bold">No Chart Data Available</p>
                            <p className="text-sm">The API did not return historical data for this symbol.</p>
                        </div>
                    )}
                </div>
                <p className="text-xs text-gray-500 p-2 text-center border-t">
                    Chart Data Source: Alpha Vantage (API Key must be configured)
                </p>
            </div>
        );
    };

    // --- RENDER ---
    
    // Ensure all necessary JS files are included for charting
    const scriptIncludes = (
        <>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/moment.js/2.29.1/moment.min.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-moment@1.0.1/dist/chartjs-adapter-moment.min.js"></script>
        </>
    );

    if (loading) {
        return (
            <div className="flex justify-center items-center h-screen bg-gray-50">
                {scriptIncludes}
                <div className="text-indigo-600 animate-spin mr-3">
                    <TrendingUp size={24} />
                </div>
                <p className="text-lg font-medium text-gray-700">Loading application...</p>
            </div>
        );
    }

    if (error && !chartData) {
        return (
            <div className="p-8 max-w-xl mx-auto mt-20 bg-red-100 border-l-4 border-red-500 text-red-700 rounded-lg shadow-lg">
                {scriptIncludes}
                <p className="font-bold">Application Error</p>
                <p>{error}</p>
                <p className="mt-2 text-sm">Please check the console for configuration details.</p>
            </div>
        );
    }

    return (
        <div className="min-h-screen bg-gray-100 p-4 font-sans antialiased">
            {scriptIncludes}
            <header className="flex justify-between items-center p-4 bg-white shadow-md rounded-lg mb-6">
                <h1 className="text-2xl font-extrabold text-indigo-700 flex items-center">
                    <DollarSign className="w-6 h-6 mr-2" />
                    Top 20 Stock Screener
                </h1>
                <div className="text-sm text-gray-600 flex items-center">
                    <LogIn className="w-4 h-4 mr-1 text-green-500" />
                    Logged in as: <span className="font-mono text-xs ml-1 text-gray-800 break-all">{userId}</span>
                </div>
            </header>

            <main className="max-w-4xl mx-auto">
                <div className="bg-white p-6 rounded-xl shadow-lg">
                    <h2 className="text-xl font-semibold mb-4 text-indigo-600 border-b pb-2">
                        Top 20 Calculated Picks
                    </h2>
                    
                    {topStocks.length === 0 ? (
                        <p className="text-gray-500 italic p-4 text-center">
                            No scored stocks available. Ensure your daily GitHub Action run finished successfully and check the Firestore path.
                        </p>
                    ) : (
                        <div className="overflow-x-auto rounded-lg border border-gray-200">
                            <table className="min-w-full divide-y divide-gray-200">
                                <thead className="bg-gray-50">
                                    <tr>
                                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Symbol</th>
                                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">P/E Ratio</th>
                                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Sentiment Score</th>
                                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Volume Surge</th>
                                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Rank Score</th>
                                        <th className="relative px-6 py-3">
                                            <span className="sr-only">Action</span>
                                        </th>
                                    </tr>
                                </thead>
                                <tbody className="bg-white divide-y divide-gray-200">
                                    {topStocks.slice(0, 20).map((stock, index) => (
                                        <tr key={stock.id} className="hover:bg-indigo-50 transition duration-150">
                                            <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-indigo-600">
                                                <span className="font-bold mr-2 text-gray-500">{index + 1}.</span>
                                                {stock.symbol || 'N/A'}
                                            </td>
                                            <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{stock.pe ? stock.pe.toFixed(1) : '—'}</td>
                                            <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{stock.sentiment ? stock.sentiment.toFixed(2) : '—'}</td>
                                            <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{stock.volumeSurge ? `${stock.volumeSurge.toFixed(1)}x` : '—'}</td>
                                            <td className="px-6 py-4 whitespace-nowrap text-sm font-bold text-right text-green-700">{stock.score ? stock.score.toFixed(3) : '0.000'}</td>
                                            <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                                                <button
                                                    onClick={() => fetchStockData(stock.symbol)}
                                                    className="inline-flex items-center px-3 py-1 border border-transparent text-sm leading-4 font-medium rounded-md text-white bg-green-500 hover:bg-green-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 transition-colors"
                                                >
                                                    View Chart
                                                </button>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}
                </div>
            </main>

            {/* Chart Modal */}
            {chartData && (
                <ChartModal data={chartData} onClose={() => setChartData(null)} />
            )}

            {/* Backdrop to close modal */}
            {chartData && (
                <div 
                    className="fixed inset-0 bg-black/10 z-40"
                    onClick={() => setChartData(null)}
                ></div>
            )}
        </div>
    );
};

export default App;
