const API_BASE = 'http://localhost:8080/api';

// Check broker status on load
window.addEventListener('DOMContentLoaded', () => {
    checkBrokerStatus();
    loadTopics();
    setInterval(checkBrokerStatus, 5000);
    setInterval(loadTopics, 10000);
});

async function checkBrokerStatus() {
    try {
        const response = await fetch(`${API_BASE}/broker/status`);
        const data = await response.json();
        
        document.getElementById('brokerId').textContent = data.brokerId || '-';
        document.getElementById('brokerPort').textContent = data.port || '-';
        document.getElementById('brokerRunning').textContent = data.running ? 'Running' : 'Stopped';
        
        const indicator = document.getElementById('statusIndicator');
        const statusText = document.getElementById('statusText');
        
        if (data.running) {
            indicator.classList.add('online');
            statusText.textContent = 'Online';
        } else {
            indicator.classList.remove('online');
            statusText.textContent = 'Offline';
        }
    } catch (error) {
        console.error('Error checking broker status:', error);
        document.getElementById('statusIndicator').classList.remove('online');
        document.getElementById('statusText').textContent = 'Error';
    }
}

async function loadTopics() {
    try {
        const response = await fetch(`${API_BASE}/topics`);
        const data = await response.json();
        
        const topicList = document.getElementById('topicList');
        
        if (data.topics && data.topics.length > 0) {
            topicList.innerHTML = data.topics.map(topic => 
                `<div class="topic-item">${topic.name}</div>`
            ).join('');
        } else {
            topicList.innerHTML = '<p>No topics created yet</p>';
        }
    } catch (error) {
        console.error('Error loading topics:', error);
        document.getElementById('topicList').innerHTML = '<p>Error loading topics</p>';
    }
}

async function createTopic() {
    const topicName = document.getElementById('topicName').value.trim();
    const partitions = parseInt(document.getElementById('partitions').value);
    const replicationFactor = parseInt(document.getElementById('replicationFactor').value);
    const resultDiv = document.getElementById('createTopicResult');
    
    if (!topicName) {
        showResult(resultDiv, 'Please enter a topic name', false);
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/topics/create`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                topicName: topicName,
                partitions: partitions,
                replicationFactor: replicationFactor
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            showResult(resultDiv, 'Topic created successfully!', true);
            document.getElementById('topicName').value = '';
            loadTopics();
        } else {
            showResult(resultDiv, data.message || 'Failed to create topic', false);
        }
    } catch (error) {
        showResult(resultDiv, 'Error: ' + error.message, false);
    }
}

async function produceMessage() {
    const topicName = document.getElementById('produceTopicName').value.trim();
    const partitionId = parseInt(document.getElementById('producePartitionId').value);
    const message = document.getElementById('produceMessage').value.trim();
    const resultDiv = document.getElementById('produceResult');
    
    if (!topicName || !message) {
        showResult(resultDiv, 'Please fill in all fields', false);
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/produce`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                topicName: topicName,
                partitionId: partitionId,
                message: message
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            showResult(resultDiv, `Message produced successfully! Offset: ${data.offset}`, true);
            document.getElementById('produceMessage').value = '';
        } else {
            showResult(resultDiv, data.error || 'Failed to produce message', false);
        }
    } catch (error) {
        showResult(resultDiv, 'Error: ' + error.message, false);
    }
}

async function fetchMessages() {
    const topicName = document.getElementById('fetchTopicName').value.trim();
    const partitionId = parseInt(document.getElementById('fetchPartitionId').value);
    const offset = parseInt(document.getElementById('fetchOffset').value);
    const maxMessages = parseInt(document.getElementById('maxMessages').value);
    const resultDiv = document.getElementById('fetchResult');
    const messagesList = document.getElementById('messagesList');
    
    if (!topicName) {
        showResult(resultDiv, 'Please enter a topic name', false);
        return;
    }
    
    try {
        const url = `${API_BASE}/fetch?topicName=${encodeURIComponent(topicName)}&partitionId=${partitionId}&offset=${offset}&maxMessages=${maxMessages}`;
        const response = await fetch(url);
        const data = await response.json();
        
        if (data.success) {
            if (data.messages && data.messages.length > 0) {
                showResult(resultDiv, `Fetched ${data.messages.length} message(s)`, true);
                messagesList.innerHTML = data.messages.map(msg => 
                    `<div class="message-item">
                        <div class="offset">Offset: ${msg.offset}</div>
                        <div class="content">${escapeHtml(msg.message)}</div>
                    </div>`
                ).join('');
            } else {
                showResult(resultDiv, 'No messages found at this offset', true);
                messagesList.innerHTML = '';
            }
        } else {
            showResult(resultDiv, data.error || 'Failed to fetch messages', false);
            messagesList.innerHTML = '';
        }
    } catch (error) {
        showResult(resultDiv, 'Error: ' + error.message, false);
        messagesList.innerHTML = '';
    }
}

function showResult(element, message, success) {
    element.textContent = message;
    element.className = 'result ' + (success ? 'success' : 'error');
    setTimeout(() => {
        element.className = 'result';
        element.textContent = '';
    }, 5000);
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}


