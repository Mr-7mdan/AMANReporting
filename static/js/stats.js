// Global variables
let isCustomizing = false;
let draggedElement = null;
let draggedElementRect = null;
let draggedElementIndex = null;
let placeholderElement = null;
let spacers = [];

// Initialize when document is ready
document.addEventListener('DOMContentLoaded', function() {
    // Initialize tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // Initialize charts
    initializeCharts();
});

// Function to initialize charts
function initializeCharts() {
    // Fetch chart data from server
    fetch('/api/charts/data')
        .then(response => response.json())
        .then(charts => {
            charts.forEach(chart => {
                const canvasId = `chart-canvas-${chart.id}`;
                const ctx = document.getElementById(canvasId)?.getContext('2d');
                if (!ctx) {
                    console.warn(`Canvas not found for chart ${chart.id}`);
                    return;
                }

                // Create chart configuration
                const config = {
                    type: chart.chart_type,
                    data: {
                        labels: chart.labels,
                        datasets: chart.datasets.map(dataset => ({
                            ...dataset,
                            borderColor: dataset.borderColor,
                            backgroundColor: dataset.backgroundColor,
                            fill: dataset.fill
                        }))
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: {
                                position: 'top',
                            },
                            title: {
                                display: false
                            },
                            tooltip: {
                                mode: 'index',
                                intersect: false,
                            }
                        },
                        scales: {
                            y: {
                                beginAtZero: true
                            }
                        }
                    }
                };

                // Create new chart
                new Chart(ctx, config);
            });
        })
        .catch(error => console.error('Error loading charts:', error));
}

// Initialize drag and drop functionality
function initializeDragAndDrop() {
    const kpiContainer = document.getElementById('kpis-container');
    const chartsContainer = document.getElementById('charts-container');
    
    // Add drag and drop event listeners to containers
    [kpiContainer, chartsContainer].forEach(container => {
        if (!container) return;
        
        container.addEventListener('dragover', handleDragOver);
        container.addEventListener('drop', handleDrop);
    });
    
    // Make cards draggable
    document.querySelectorAll('.kpi-card, .chart-container').forEach(card => {
        card.setAttribute('draggable', 'true');
        card.addEventListener('dragstart', handleDragStart);
        card.addEventListener('dragend', handleDragEnd);
    });
}

// Toggle customize mode
function toggleCustomizeMode() {
    isCustomizing = !isCustomizing;
    const customizeBtn = document.getElementById('customize-btn');
    
    if (isCustomizing) {
        customizeBtn.classList.add('active');
        document.body.classList.add('customize-mode');
        initializeDragAndDrop();
    } else {
        customizeBtn.classList.remove('active');
        document.body.classList.remove('customize-mode');
        removeSpacers();
    }
}

// Save layout
function saveLayout() {
    const layout = {
        kpis: Array.from(document.querySelectorAll('#kpis-container .kpi-card')).map(card => ({
            id: card.dataset.kpiId,
            type: Array.from(card.classList).find(c => c.endsWith('-kpi')),
            order: Array.from(card.parentNode.children).indexOf(card)
        })),
        charts: Array.from(document.querySelectorAll('#charts-container .chart-container')).map(chart => ({
            id: chart.dataset.chartId,
            order: Array.from(chart.parentNode.children).indexOf(chart)
        }))
    };

    fetch('/api/save_layout', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(layout)
    })
    .then(response => response.json())
    .then(data => {
        if (data.error) {
            console.error('Error saving layout:', data.error);
        }
    })
    .catch(error => console.error('Error saving layout:', error));
}

// Drag and drop handlers
function handleDragStart(e) {
    if (!isCustomizing) return;
    
    draggedElement = e.target;
    draggedElementRect = draggedElement.getBoundingClientRect();
    draggedElementIndex = Array.from(draggedElement.parentNode.children).indexOf(draggedElement);
    
    e.dataTransfer.effectAllowed = 'move';
    e.dataTransfer.setData('text/plain', ''); // Required for Firefox
    
    // Create placeholder
    placeholderElement = document.createElement('div');
    placeholderElement.className = 'kpi-placeholder';
    placeholderElement.style.width = draggedElementRect.width + 'px';
    placeholderElement.style.height = draggedElementRect.height + 'px';
    
    // Add class for styling during drag
    draggedElement.classList.add('dragging');
    
    // Create spacers
    createSpacers();
    
    // Delay to ensure proper visual feedback
    requestAnimationFrame(() => {
        draggedElement.style.opacity = '0.5';
    });
}

function handleDragEnd(e) {
    if (!isCustomizing) return;
    
    draggedElement.classList.remove('dragging');
    draggedElement.style.opacity = '';
    
    removeSpacers();
    
    if (placeholderElement && placeholderElement.parentNode) {
        placeholderElement.parentNode.removeChild(placeholderElement);
    }
    
    draggedElement = null;
    draggedElementRect = null;
    draggedElementIndex = null;
    placeholderElement = null;
    
    // Save the new layout
    saveLayout();
}

function handleDragOver(e) {
    if (!isCustomizing || !draggedElement) return;
    
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
    
    const closestSpacer = getClosestSpacer(e.clientX, e.clientY);
    if (closestSpacer) {
        spacers.forEach(spacer => {
            spacer.style.borderColor = '#dee2e6';
            spacer.style.background = 'transparent';
        });
        closestSpacer.style.borderColor = '#0d6efd';
        closestSpacer.style.background = 'rgba(13, 110, 253, 0.05)';
    }
}

function handleDrop(e) {
    if (!isCustomizing || !draggedElement) return;
    
    e.preventDefault();
    
    const closestSpacer = getClosestSpacer(e.clientX, e.clientY);
    if (closestSpacer) {
        const container = closestSpacer.parentNode;
        container.insertBefore(draggedElement, closestSpacer);
    }
    
    handleDragEnd(e);
}

// Helper functions
function createSpacers() {
    const containers = [
        document.getElementById('kpis-container'),
        document.getElementById('charts-container')
    ];
    
    containers.forEach(container => {
        if (!container) return;
        
        const items = Array.from(container.children);
        items.forEach((item, index) => {
            if (item === draggedElement) return;
            
            const spacer = document.createElement('div');
            spacer.className = 'space-placeholder';
            spacer.dataset.index = index;
            container.insertBefore(spacer, item);
            spacers.push(spacer);
        });
        
        // Add final spacer
        const finalSpacer = document.createElement('div');
        finalSpacer.className = 'space-placeholder';
        finalSpacer.dataset.index = items.length;
        container.appendChild(finalSpacer);
        spacers.push(finalSpacer);
    });
}

function removeSpacers() {
    spacers.forEach(spacer => {
        if (spacer.parentNode) {
            spacer.parentNode.removeChild(spacer);
        }
    });
    spacers = [];
}

function getClosestSpacer(x, y) {
    let closestSpacer = null;
    let closestDistance = Infinity;
    
    spacers.forEach(spacer => {
        const rect = spacer.getBoundingClientRect();
        const distance = Math.hypot(
            x - (rect.left + rect.width / 2),
            y - (rect.top + rect.height / 2)
        );
        
        if (distance < closestDistance) {
            closestDistance = distance;
            closestSpacer = spacer;
        }
    });
    
    return closestSpacer;
}
